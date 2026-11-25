---
mkskill:
  pos: 5
  in: ai*
---

# quevu-sqs - agent notes

Go package `github.com/pablo-botella/quevu-sqs`, imported as `qsqs`:
`qsqs.Writer` (a `qkernel.Writer`) and `qsqs.Source` (a `qkernel.Unit`
and `qkernel.Exiter` that feeds a `qkernel.Intake`). Depends on `quevu` and on
the AWS SDK for Go v2 (`service/sqs`, `config`, `credentials`), and on no
other provider's SDK.

Everything goes through the small `api` interface, which is what the tests
replace. The module has never been run against Amazon: treat any claim
about the service as coming from the SDK's documentation, not from
experience.
