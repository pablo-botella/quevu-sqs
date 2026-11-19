# quevu-sqs - agent notes

Go package `github.com/pablo-botella/quevu-sqs`, imported as `qsqs`:
`qsqs.Consumer` (a `quevu.Consumer`) and `qsqs.Source` (a `quevu.Unit`
and `quevu.Exiter` that feeds a `quevu.Intake`). Depends on `quevu` and on
the AWS SDK for Go v2 (`service/sqs`, `config`, `credentials`), and on no
other provider's SDK.

Everything goes through the small `api` interface, which is what the tests
replace. The module has never been run against Amazon: treat any claim
about the service as coming from the SDK's documentation, not from
experience.

## Two directions, one service

Sending and receiving use the same service, the same credentials and
usually the same queue, with different operations:

- **Sending** is one call: a batch of messages goes to the queue.
- **Receiving** is two: a message that has been received stays in the
  queue, hidden for a while, until it is deleted. If it is not deleted it
  comes back.

So receiving is *at least once*. The source deletes a message only after
the log has accepted it (`quevu.Intake.Append` returned without error).
The worst that can happen is that a message arrives twice; it is never
lost between the queue and the log.

One chunk is one message.

## Configuration

```go
qsqs.Config{
	QueueURL:        "...", // required
	Region:          "",    // empty: from the environment
	AccessKeyID:     "",    // empty: from the environment
	SecretAccessKey: "",
	SessionToken:    "",
	Endpoint:        "",    // an emulator or a private endpoint
}
```

What is left empty is looked for the way the AWS SDK does: environment
variables, the shared config and credentials files, the role of the
machine. Credentials belong in the configuration of the deployment.
**Never write real ones in source code, in an example or in a test.**

## What travels

- Text that SQS accepts (valid UTF-8 made of the characters XML allows)
  travels as it is, so the queue stays readable.
- Anything else travels as base64, with a message attribute saying so,
  and comes back as the original bytes. An empty chunk travels as a mark:
  a message needs at least one character.
- Every message carries the position and the time the chunk had in its
  log (`quevu-seq`, `quevu-time`). The receiving log gives it a position
  of its own.
- A message that did not come from quevu is taken as it is.

## Limits

- A message, and the total of one batch call, is at most **1 MiB**; a
  call carries at most **10** messages. Batches are split to fit.
- **A chunk too large for a message is not sent and not split.** `Write`
  returns `ErrTooLarge` for it and sends the rest; the log counts the
  batch as failed.

## Rules and gotchas

- **Duplicates are possible** on the input side. If that matters, the
  application must cope with it.
- **Order is not guaranteed** on a standard queue. FIFO queues need a
  message group, which this module does not set.
- **The service being out of reach does not end the source:** it waits
  and tries again. A log that refuses a message does end it, with that
  error, and the message stays in the queue.
- **`Stop` finishes the batch in hand**: what was received is handed to
  the log and deleted before it returns.
- **Emulators** (LocalStack) take `Config.Endpoint`. Not tried.
