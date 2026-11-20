---
mkskill:
  pos: 10
  in: readme
---

# quevu-sqs

Amazon SQS as [quevu](https://github.com/pablo-botella/quevu-kernel) components,
in both directions.

> **Not verified against the real service.** It is tested against a
> stand-in that keeps the limits of SQS as its SDK documents them. Nothing
> here has talked to Amazon yet.

```go
cfg := qsqs.Config{QueueURL: os.Getenv("ORDERS_QUEUE_URL")} // the rest from the environment

// Output: a writer of a log.
out, _ := qsqs.NewWriter(ctx, cfg)
orders, _ := qhub.New("orders", qhub.WithWriter(out))

// Input: a unit that feeds a log.
in, _ := qsqs.NewSource(ctx, cfg, incoming)
panel.Add("orders queue", in)
```
