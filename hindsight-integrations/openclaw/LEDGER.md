# Plugin ledger

Decision records for behaviour that cannot be inferred from the code alone.

## 2026-11-10 — heartbeat derivation for hosts that omit `isHeartbeat`

**Issue.** Durable turn advancement passes `CommitTurnParams`, and upstream main never populates
`isHeartbeat` (the outbox `commonParams` has no such field, and heartbeats migrate to cron monitor
jobs). `commitTurn` therefore sees `heartbeat: false` for every turn on the new host, which changes
three things that production's 10.3 host still gets right: the ledger `heartbeat` column, retain
queueing, and the package-28 classification spawn.

**Gate and default.** `deriveHeartbeatFromSessionKey` (config, default **false**). When true,
`commitTurn` derives heartbeat from the session key through `isOperationalSessionKey`, the same
helper `getIdentitySkipReason` uses for `skipOperationalSessions`, so the two cannot drift.

**Our deployed value.** `true` on openclaw2 now and in production at cutover. Off is only
behaviour-preserving for hosts that send `isHeartbeat`; on the new host, off is the change and on
is what keeps today's behaviour.

**Evidence.** `tsc` exit 0; `durable-turns` 20/20 including a test that commits one cron-keyed
turn twice (flag off, then on) and asserts the ledger heartbeat false then true; full suite 52
files passed, 1 skipped.

**Known limit.** With `isolatedSession: false` (C4), cron monitor jobs run on the owning desk's
session key, which the operational pattern does not match. Those turns still count as ordinary
turns. Only isolated `agent:<id>:(cron|heartbeat):` keys are caught. Accepted: those wake turns
run with the desk's own context.
