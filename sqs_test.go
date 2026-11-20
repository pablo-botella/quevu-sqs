package qsqs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	quevu "github.com/pablo-botella/quevu-kernel"
)

// These tests run against a stand-in for the service that keeps its
// rules: ten messages and one mebibyte per call, a message stays until
// deleted. They prove the logic of this package, not that the real
// service agrees with it: nothing here has talked to Amazon.

type queue struct {
	mu       sync.Mutex
	messages []types.Message
	calls    []int // messages per send call
	deleted  []string
	refuse   map[string]bool // bodies the service refuses
	down     bool
	received chan struct{}
}

func newQueue() *queue { return &queue{refuse: map[string]bool{}, received: make(chan struct{}, 100)} }

func (q *queue) SendMessageBatch(_ context.Context, in *sqs.SendMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.down {
		return nil, errors.New("service unavailable")
	}
	if len(in.Entries) > 10 {
		return nil, errors.New("TooManyEntriesInBatchRequest")
	}
	total := 0
	for _, e := range in.Entries {
		total += len(aws.ToString(e.MessageBody))
	}
	if total > MaxMessage {
		return nil, errors.New("BatchRequestTooLong")
	}
	q.calls = append(q.calls, len(in.Entries))
	out := &sqs.SendMessageBatchOutput{}
	for _, e := range in.Entries {
		if q.refuse[aws.ToString(e.MessageBody)] {
			out.Failed = append(out.Failed, types.BatchResultErrorEntry{Id: e.Id, Code: aws.String("InvalidMessageContents"), Message: aws.String("no")})
			continue
		}
		id := fmt.Sprintf("m%d", len(q.messages)+1)
		q.messages = append(q.messages, types.Message{
			MessageId: aws.String(id), ReceiptHandle: aws.String("rh-" + id),
			Body: e.MessageBody, MessageAttributes: e.MessageAttributes,
		})
	}
	return out, nil
}

func (q *queue) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	q.mu.Lock()
	down := q.down
	var out []types.Message
	if !down {
		for _, m := range q.messages {
			if len(out) == int(in.MaxNumberOfMessages) {
				break
			}
			out = append(out, m)
		}
	}
	q.mu.Unlock()
	select {
	case q.received <- struct{}{}:
	default:
	}
	if down {
		return nil, errors.New("service unavailable")
	}
	if len(out) == 0 {
		// A long poll with nothing to give.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return &sqs.ReceiveMessageOutput{Messages: out}, nil
}

func (q *queue) DeleteMessageBatch(_ context.Context, in *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, e := range in.Entries {
		h := aws.ToString(e.ReceiptHandle)
		q.deleted = append(q.deleted, h)
		for i, m := range q.messages {
			if aws.ToString(m.ReceiptHandle) == h {
				q.messages = append(q.messages[:i], q.messages[i+1:]...)
				break
			}
		}
	}
	return &sqs.DeleteMessageBatchOutput{}, nil
}

func (q *queue) waiting() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages)
}

func chunks(texts ...string) []quevu.Chunk {
	out := make([]quevu.Chunk, len(texts))
	for i, t := range texts {
		out[i] = quevu.Chunk{Seq: uint64(i + 1), Time: time.Unix(1700000000, int64(i)), Data: []byte(t)}
	}
	return out
}

// log stands for a quevu log: it keeps what it accepts.
type log struct {
	mu     sync.Mutex
	got    []string
	refuse string
}

func (l *log) Write(p []byte) (int, error) { return len(p), nil }

func (l *log) Append(data []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.refuse != "" && string(data) == l.refuse {
		return 0, errors.New("log closed")
	}
	l.got = append(l.got, string(data))
	return uint64(len(l.got)), nil
}

func (l *log) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.got...)
}

func wait(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); !ok(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("never happened: " + what)
		}
	}
}

func TestOneChunkOneMessageWithinTheLimitsOfACall(t *testing.T) {
	q := newQueue()
	c := &Consumer{api: q, queue: "q", timeout: time.Second}
	var texts []string
	for i := 0; i < 23; i++ {
		texts = append(texts, fmt.Sprintf("line %d", i))
	}
	if err := c.Write(chunks(texts...)); err != nil {
		t.Fatal(err)
	}
	if q.waiting() != 23 {
		t.Fatalf("%d messages in the queue", q.waiting())
	}
	if fmt.Sprint(q.calls) != "[10 10 3]" {
		t.Fatalf("calls: %v", q.calls)
	}
	// Where a chunk came from travels with it.
	m := q.messages[4]
	if aws.ToString(m.Body) != "line 4" || aws.ToString(m.MessageAttributes[attrSeq].StringValue) != "5" {
		t.Fatalf("message: %s %v", aws.ToString(m.Body), m.MessageAttributes)
	}
	if c.Close() != nil {
		t.Fatal("close")
	}
}

func TestACallNeverCarriesMoreThanTheServiceTakes(t *testing.T) {
	q := newQueue()
	c := &Consumer{api: q, queue: "q", timeout: time.Second}
	// Three chunks of 400 KiB do not fit in one call of 1 MiB.
	big := strings.Repeat("x", 400<<10)
	if err := c.Write(chunks(big, big, big, "small")); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(q.calls) != "[2 2]" {
		t.Fatalf("calls: %v", q.calls)
	}
}

func TestAChunkTooLargeIsLeftOutAndSaid(t *testing.T) {
	q := newQueue()
	c := &Consumer{api: q, queue: "q", timeout: time.Second}
	err := c.Write(chunks("before", strings.Repeat("x", MaxMessage+1), "after"))
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v", err)
	}
	if q.waiting() != 2 {
		t.Fatalf("%d messages: the rest of the batch should have gone", q.waiting())
	}
}

func TestWhatIsNotTextTravelsEncodedAndComesBackWhole(t *testing.T) {
	q := newQueue()
	c := &Consumer{api: q, queue: "q", timeout: time.Second}
	in := []string{"text áñ \U0001f600", "\x00\xff binary", "esc \x1b[31m color", ""}
	if err := c.Write(chunks(in...)); err != nil {
		t.Fatal(err)
	}
	for i, m := range q.messages {
		_, encoded := m.MessageAttributes[attrEncoding]
		if encoded != (i > 0) {
			t.Fatalf("message %d encoded=%v", i, encoded)
		}
		if !sendable([]byte(aws.ToString(m.Body))) {
			t.Fatalf("message %d would be refused by the service: %q", i, aws.ToString(m.Body))
		}
		data, err := decode(m)
		if err != nil || string(data) != in[i] {
			t.Fatalf("message %d came back as %q, %v", i, data, err)
		}
	}
}

func TestRefusalsByTheServiceAreErrors(t *testing.T) {
	q := newQueue()
	q.refuse["bad"] = true
	c := &Consumer{api: q, queue: "q", timeout: time.Second}
	if err := c.Write(chunks("good", "bad")); err == nil || !strings.Contains(err.Error(), "1 of 2") {
		t.Fatalf("got %v", err)
	}
	q.down = true
	if err := c.Write(chunks("x")); err == nil {
		t.Fatal("no error with the service down")
	}
}

func TestAMessageIsDeletedOnlyAfterTheLogTookIt(t *testing.T) {
	q := newQueue()
	c := &Consumer{api: q, queue: "q", timeout: time.Second}
	c.Write(chunks("one", "two", "\x00binary", "four"))
	var l log
	s := &Source{api: q, queue: "q", into: &l, wait: 1, pause: 10 * time.Millisecond}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); err == nil {
		t.Fatal("started twice")
	}
	wait(t, "four messages in the log", func() bool { return len(l.all()) == 4 })
	wait(t, "the queue emptied", func() bool { return q.waiting() == 0 })
	if got := strings.Join(l.all(), "|"); got != "one|two|\x00binary|four" {
		t.Fatalf("log: %q", got)
	}
	if s.Taken() != 4 {
		t.Fatalf("taken %d", s.Taken())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-s.Done(); err != nil {
		t.Fatalf("a stop reported as a failure: %v", err)
	}
}

func TestWhatTheLogRefusesStaysInTheQueue(t *testing.T) {
	q := newQueue()
	c := &Consumer{api: q, queue: "q", timeout: time.Second}
	c.Write(chunks("one", "two", "three"))
	l := &log{refuse: "two"}
	s := &Source{api: q, queue: "q", into: l, wait: 1, pause: 10 * time.Millisecond}
	s.Start(context.Background())
	select {
	case err := <-s.Done():
		if err == nil || !strings.Contains(err.Error(), "log closed") {
			t.Fatalf("got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the source went on with a log that takes nothing")
	}
	// "one" was accepted and is gone; the rest wait for another day.
	if got := strings.Join(l.all(), "|"); got != "one" {
		t.Fatalf("log: %q", got)
	}
	if q.waiting() != 2 || aws.ToString(q.messages[0].Body) != "two" {
		t.Fatalf("%d messages left in the queue", q.waiting())
	}
}

func TestTheServiceBeingDownIsWaitedOut(t *testing.T) {
	q := newQueue()
	q.down = true
	var l log
	s := &Source{api: q, queue: "q", into: &l, wait: 1, pause: 10 * time.Millisecond}
	s.Start(context.Background())
	<-q.received
	<-q.received
	// It is still trying, not dead.
	select {
	case err := <-s.Done():
		t.Fatalf("the source gave up: %v", err)
	default:
	}
	q.mu.Lock()
	q.down = false
	q.messages = append(q.messages, types.Message{MessageId: aws.String("x"), ReceiptHandle: aws.String("rh-x"), Body: aws.String("back")})
	q.mu.Unlock()
	wait(t, "the message after the outage", func() bool { return len(l.all()) == 1 })
	s.Stop(context.Background())
}

func TestConfigIsChecked(t *testing.T) {
	if _, err := NewConsumer(context.Background(), Config{}); err == nil {
		t.Fatal("no queue URL accepted")
	}
	if _, err := NewSource(context.Background(), Config{QueueURL: "https://example.com/q"}, nil); err == nil {
		t.Fatal("no log accepted")
	}
	// Building a client needs no network and no account.
	c, err := NewConsumer(context.Background(), Config{
		QueueURL: "https://sqs.eu-west-1.amazonaws.com/000000000000/example",
		Region:   "eu-west-1", AccessKeyID: "EXAMPLE", SecretAccessKey: "example",
		Endpoint: "http://127.0.0.1:1",
	})
	if err != nil || c == nil {
		t.Fatalf("client: %v", err)
	}
}
