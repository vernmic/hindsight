# OpenClaw integration boundaries

Native memory-provider registration is already upstream in #5535. The standard plugin does not
require our Astinus customization. Use the upstream integration's documentation for supported
installation rather than treating this fork's historical default branch as the latest package.

The custom integration retains legacy `agent_knowledge_*` adapters, context reduction,
host-bound sidecars and durable source accounting. Its current local qualification pair is host
`a20345f3aa0112c53d8f5b447a0bd8e913bcfb69` plus plugin
`e8cb94b7a8de11857089d680db7b7407b63f2e97`. The ownership signal is a paired host prerequisite;
do not claim compatibility with every stock host. These local identities are not advertised as
published or runtime-qualified artifacts by this document.

The custom knowledge adapter separates `max_results` (count, default10) from `max_tokens`
(context budget, configured or fallback1024). Raising the count does not override the token budget.
It uses retained SDK0.1.0 tools that upstream removed in #5462; that adapter is distinct from
native provider search/get and the separate Hermes cap in issue5426.

Durable retain records before sending, binds destination/credential identity, distinguishes
accepted operations from terminal success and retires exact local debt after completion.
Lost acknowledgements reuse operation identity. Uncertain or failed outcomes preserve debt.
Nonblocking capture does not remove those obligations. Queues and sidecars use the loaded host's
explicit root, and credentials are bound to their intended provider.

The corrected source/companion/artifact have independent review; loaded status/search/get,
canonical commit/delivery, source retirement, real pending recovery across joined stop/start,
session discovery, desk/UI routes and owned child cleanup remain separate runtime claims.
At the start of this publication work the new pair was not runtime-qualified. Registration and
on-disk staging do not close that gate. Production cutover, compaction ownership and deferred
supervision are outside the publication work order. No raw transcript, queue, database,
credential, private journal or machine path is included as public evidence.
