package qsqs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	quevu "github.com/pablo-botella/quevu-kernel"
)

// Source reads a queue and feeds what arrives to a log.
type Source struct {
	api   api
	queue string
	into  quevu.Intake
	// wait is how long one receive call waits for a message before
	// coming back empty. The service allows up to twenty seconds.
	wait int32
	// pause is how long to wait after a failed call before trying again.
	pause time.Duration

	mu     sync.Mutex
	cancel context.CancelFunc
	fin    chan struct{}
	done   chan error

	taken atomic.Uint64
}

// NewSource connects to the queue for receiving. into is the log every
// message is appended to, as one chunk each.
func NewSource(ctx context.Context, cfg Config, into quevu.Intake) (*Source, error) {
	if into == nil {
		return nil, errors.New("qsqs: no log to feed")
	}
	a, err := connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Source{api: a, queue: cfg.QueueURL, into: into, wait: 20, pause: 5 * time.Second}, nil
}

// Start begins receiving. It returns at once.
func (s *Source) Start(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fin != nil {
		select {
		case <-s.fin:
		default:
			return errors.New("qsqs: already running")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	fin := make(chan struct{})
	done := make(chan error, 1)
	s.cancel, s.fin, s.done = cancel, fin, done
	go func() {
		err := s.run(ctx)
		cancel()
		done <- err
		close(done)
		close(fin)
	}()
	return nil
}

// run receives until it is told to stop or the log refuses a message.
func (s *Source) run(ctx context.Context) error {
	for ctx.Err() == nil {
		out, err := s.api.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(s.queue),
			MaxNumberOfMessages:   maxBatch,
			WaitTimeSeconds:       s.wait,
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// The service being out of reach is not the end of the
			// input: it tries again after a pause.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(s.pause):
			}
			continue
		}
		if err := s.take(out.Messages); err != nil {
			return err
		}
	}
	return nil
}

// take hands messages to the log and deletes from the queue the ones the
// log accepted. The order matters: a message is deleted only once it is
// in the log, so the worst that can happen is that it arrives twice.
func (s *Source) take(msgs []types.Message) error {
	var accepted []types.DeleteMessageBatchRequestEntry
	var refused error
	for i, m := range msgs {
		data, err := decode(m)
		if err == nil {
			_, err = s.into.Append(data)
		}
		if err != nil {
			// Not deleted: it stays in the queue and comes back.
			refused = fmt.Errorf("qsqs: the log did not take a message: %w", err)
			break
		}
		s.taken.Add(1)
		accepted = append(accepted, types.DeleteMessageBatchRequestEntry{
			Id:            aws.String(fmt.Sprint(i)),
			ReceiptHandle: m.ReceiptHandle,
		})
	}
	if len(accepted) > 0 {
		// Even while stopping: these are in the log already.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// A delete that fails is not fatal: the message comes back and
		// is taken again.
		s.api.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String(s.queue),
			Entries:  accepted,
		})
	}
	return refused
}

// Stop ends the receiving in an orderly way: a batch already received is
// handed to the log and confirmed before it returns.
func (s *Source) Stop(ctx context.Context) error {
	s.mu.Lock()
	cancel, fin := s.cancel, s.fin
	s.mu.Unlock()
	if fin == nil {
		return nil
	}
	cancel()
	select {
	case <-fin:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Done yields the result of the current run when it ends.
func (s *Source) Done() <-chan error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

// Taken returns how many messages the log has accepted.
func (s *Source) Taken() uint64 { return s.taken.Load() }

var (
	_ quevu.Unit   = (*Source)(nil)
	_ quevu.Exiter = (*Source)(nil)
)
