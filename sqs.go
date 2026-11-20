// Package qsqs connects quevu to Amazon SQS in both directions.
//
//   - Writer is a writer of a hub (quevu.Writer): what the log
//     distributes is sent to a queue.
//   - Source is an input (a quevu.Unit): what arrives at a queue is fed
//     to a log, and follows the normal way to its writers from there.
//
// The two use the same service, the same credentials and usually the
// same queue, with different operations: sending is one call, receiving
// is two, because a message that has been received stays in the queue
// until it is deleted. If it is not deleted it comes back. So receiving
// is "at least once": the source deletes a message only after the log
// has accepted it, and a message may arrive twice.
//
// One chunk is one message.
package qsqs

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	quevu "github.com/pablo-botella/quevu-kernel"
)

const (
	// MaxMessage is the largest message SQS takes, body and attributes
	// together, and also the largest total payload of one batch call.
	MaxMessage = 1 << 20
	// maxBatch is how many messages one batch call may carry.
	maxBatch = 10
	// overhead is room left in a message for the attributes this
	// package adds.
	overhead = 512

	attrSeq      = "quevu-seq"
	attrTime     = "quevu-time"
	attrEncoding = "quevu-encoding"
)

// ErrTooLarge is returned for a chunk that does not fit in one message.
// The chunk is not sent and not split: the log counts it as failed.
var ErrTooLarge = errors.New("qsqs: chunk larger than an SQS message")

// Config says which queue, and as whom. Nothing here has a default taken
// from code: what is left empty comes from the environment, the way the
// AWS SDK looks for it (environment variables, shared config and
// credentials files, the role of the machine).
type Config struct {
	// QueueURL is the URL of the queue, as SQS gives it.
	QueueURL string
	// Region is the AWS region. Empty: from the environment.
	Region string
	// AccessKeyID, SecretAccessKey and SessionToken are static
	// credentials. All empty: from the environment. They belong in the
	// configuration of the deployment, never in source code.
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	// Endpoint overrides the address of the service, for an emulator or
	// a private endpoint. Empty: the real service of the region.
	Endpoint string
}

// api is the part of the SQS client this package uses. It is what tests
// replace.
type api interface {
	SendMessageBatch(ctx context.Context, in *sqs.SendMessageBatchInput, opts ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, in *sqs.DeleteMessageBatchInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
}

func connect(ctx context.Context, cfg Config) (api, error) {
	if cfg.QueueURL == "" {
		return nil, errors.New("qsqs: no queue URL")
	}
	var opts []func(*config.LoadOptions) error
	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}
	if cfg.AccessKeyID != "" || cfg.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)))
	}
	ac, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("qsqs: %w", err)
	}
	return sqs.NewFromConfig(ac, func(o *sqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	}), nil
}

// sendable reports whether data can travel as the text of a message: SQS
// takes only valid UTF-8 made of the characters XML allows.
func sendable(data []byte) bool {
	if len(data) == 0 || !utf8.Valid(data) {
		return false
	}
	for _, r := range string(data) {
		switch {
		case r == 0x9 || r == 0xA || r == 0xD:
		case r >= 0x20 && r <= 0xD7FF:
		case r >= 0xE000 && r <= 0xFFFD:
		case r >= 0x10000:
		default:
			return false
		}
	}
	return true
}

// encode turns a chunk into a message body and its attributes. Text
// travels as it is; anything else travels as base64 and says so.
func encode(c quevu.Chunk) (body string, attrs map[string]types.MessageAttributeValue) {
	attrs = map[string]types.MessageAttributeValue{
		attrSeq:  {DataType: aws.String("Number"), StringValue: aws.String(strconv.FormatUint(c.Seq, 10))},
		attrTime: {DataType: aws.String("Number"), StringValue: aws.String(strconv.FormatInt(c.Time.UnixNano(), 10))},
	}
	if sendable(c.Data) {
		return string(c.Data), attrs
	}
	if len(c.Data) == 0 {
		// A message needs at least one character, and an empty line is
		// still a line: it travels as a mark that says so.
		attrs[attrEncoding] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("empty")}
		return "-", attrs
	}
	attrs[attrEncoding] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("base64")}
	return base64.StdEncoding.EncodeToString(c.Data), attrs
}

// decode gives back the data of a message, undoing what encode did. A
// message that did not come from quevu is taken as it is.
func decode(m types.Message) ([]byte, error) {
	body := aws.ToString(m.Body)
	enc := aws.ToString(m.MessageAttributes[attrEncoding].StringValue)
	if enc == "empty" {
		return nil, nil
	}
	if enc == "base64" {
		data, err := base64.StdEncoding.DecodeString(body)
		if err != nil {
			return nil, fmt.Errorf("qsqs: message %s: %w", aws.ToString(m.MessageId), err)
		}
		return data, nil
	}
	return []byte(body), nil
}

// Writer sends the chunks of a log to a queue.
type Writer struct {
	fails quevu.Failures // the body of Run, and what it could not write

	api   api
	queue string
	// timeout bounds one call to the service.
	timeout time.Duration
}

// NewWriter connects to the queue for sending.
func NewWriter(ctx context.Context, cfg Config) (*Writer, error) {
	a, err := connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Writer{api: a, queue: cfg.QueueURL, timeout: 30 * time.Second}, nil
}

// Write sends a batch, one message per chunk, in as few calls as the
// limits of the service allow. A chunk too large for a message is left
// out and reported with ErrTooLarge; the rest are sent.
func (c *Writer) Write(batch []quevu.Chunk) error {
	var errs []error
	var entries []types.SendMessageBatchRequestEntry
	size := 0
	flush := func() {
		if len(entries) == 0 {
			return
		}
		if err := c.send(entries); err != nil {
			errs = append(errs, err)
		}
		entries, size = nil, 0
	}
	for i, ch := range batch {
		body, attrs := encode(ch)
		n := len(body) + overhead
		if n > MaxMessage {
			errs = append(errs, fmt.Errorf("%w: position %d, %d bytes", ErrTooLarge, ch.Seq, len(ch.Data)))
			continue
		}
		if len(entries) == maxBatch || size+n > MaxMessage {
			flush()
		}
		entries = append(entries, types.SendMessageBatchRequestEntry{
			Id:                aws.String(strconv.Itoa(i)),
			MessageBody:       aws.String(body),
			MessageAttributes: attrs,
		})
		size += n
	}
	flush()
	return errors.Join(errs...)
}

func (c *Writer) send(entries []types.SendMessageBatchRequestEntry) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	out, err := c.api.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(c.queue),
		Entries:  entries,
	})
	if err != nil {
		return fmt.Errorf("qsqs: send: %w", err)
	}
	if n := len(out.Failed); n > 0 {
		f := out.Failed[0]
		return fmt.Errorf("qsqs: %d of %d messages refused; first: %s %s",
			n, len(entries), aws.ToString(f.Code), aws.ToString(f.Message))
	}
	return nil
}

// Close does nothing: the client holds no connection to release.
func (c *Writer) Close() error { return nil }

var _ quevu.Writer = (*Writer)(nil)

// Run receives from the channel the hub gave this writer until it is
// closed, writing what arrives in batches, and closes the writer at the
// end. It is what makes it a quevu.Writer.
func (c *Writer) Run(in <-chan quevu.Chunk) error { return c.fails.Drain(in, 0, c) }

// Failed returns the chunks lost so far in failed batches, and the last
// error.
func (c *Writer) Failed() (uint64, error) { return c.fails.Failed() }
