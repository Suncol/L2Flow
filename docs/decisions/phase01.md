# Phase 0–1 implementation decisions

This record narrows `docs/design.md` into the contracts implemented before the
portable Callback WAL and decoder phases exist.

## Phase 0 gate

The approved facts are compiled constants and are rendered byte-exactly as
`configs/vendor_baseline.json`. A runtime JSON edit cannot relax the gate.
The baseline file itself is required to have the compiled exact size, captured
once into a sealed snapshot, and byte-compared on that retained descriptor;
the report does not reopen the mutable pathname to obtain a second identity.
The archive hash uses one `O_NOFOLLOW|O_NONBLOCK` regular-file descriptor and a
1 GiB pre-read operational limit. The limit bounds hashing work; the approved
SHA-256, not the cap, is the artifact identity.

Before the slower full gate, startup opens the configured library with
`O_NOFOLLOW|O_NONBLOCK`, requires a regular file no larger than the frozen
1 GiB operational bound before allocating or populating a memfd, copies its
exact bytes into a close-on-exec memfd, verifies that source metadata did not
change during the copy, and applies
`F_SEAL_WRITE|F_SEAL_GROW|F_SEAL_SHRINK|F_SEAL_SEAL`. The gate then verifies:

1. the baseline file bytes;
2. the original SDK archive SHA-256;
3. the candidate shared library is one bounded, sealed immutable snapshot;
4. constrained ELF64 class/data/type/machine, a present diagnostic Build ID,
   absent SONAME, the exact dependency set, and the exact imported
   symbol-version compatibility set;
5. C++20 host properties and frozen vendor sizes, alignments, offsets, enum
   literals, and message keys;
6. only after all previous checks, `dlopen`/`dlsym`, a wrong-version
   `DllCreateIOManager` call, and the 2.13.234 create/shutdown/release
   call.

Schema v2 deliberately pins the reviewed SDK header archive and ABI contract,
not one shared-object hash or compiler producer string. A different 2.13.234
library build is accepted only if every structural ELF, dependency,
symbol-version, compiled ABI, and runtime lifecycle check passes.

Concretely, service startup performs that full preflight on sealed snapshot A.
The production loader independently captures fresh sealed snapshot B, repeats
the library snapshot/ELF/ABI/runtime component gate against B's retained
descriptor, and performs final `dlopen`/`dlsym` on B. Thus each component
gate and its load consume one immutable byte identity; A and B are deliberately
separate compatibility-checked captures rather than one hash identity.
Snapshot B and the dynamic handle remain owned for the SDK-object lifetime.
Path replacement, truncation, mutation, or unlink after either capture cannot
redirect that capture's checked bytes. Environments without Linux memfd sealing
or usable `/proc/self/fd` support fail closed.

The build manifest has no generation timestamp. It records compiler identity,
compiler path/version, target system, build type, C++ standard, exact strict
and sanitizer flags, the baseline JSON hash, and a real source revision or an
explicit unavailable value.

## Phase 1 process and callback boundary

There are four compiled executables. Each process constructs one manager and
one subscriber. Shenzhen messages 6.33 and 6.36 deliberately share the same
subscriber/ring order. No service exposes `SetPassword` or
`SetReadBufferSize`; `send_mac_auth` comes from the endpoint contract.

The documented feeder and direct upstream protocol place the account token in
`SetUserName`; `SetPassword` remains outside the reviewed surface. The local
feeder probe uses a fixed non-secret label because the cascade publisher is an
anonymous local boundary. On shutdown, the application's Subscriber reference
may release to zero or remain positive while IOManager still owns its registry
reference; a negative count is invalid. The IOManager factory reference must
still release exactly to zero.


The production CLI accepts only an absolute endpoint-contract path and its
expected lowercase SHA-256. It does not accept address, encoding,
`merge_message`, `send_mac_auth`, or `server_select` overrides. The version-1
contract is strict JSON with exactly these fields:

```text
schema_version, ingress_kind, name, resolved_server_address,
message_encoding, merge_message, send_mac_auth, server_select
```

The loader opens the final component with `O_NOFOLLOW`, requires a regular
file no larger than 4096 bytes, reads the exact file length, hashes and parses
those same bytes, and checks the service kind. Unknown, duplicate, missing,
escaped, malformed UTF-8, or wrongly typed fields fail before credentials or
SDK loading.

Production startup repeats a strict filesystem path policy after resolving
the credential path. Every service path must be absolute and NUL-free, contain
no `.`/`..` or empty components, use a portable ASCII basename, and have a
pre-existing parent. Shadow and metrics outputs may not alias each other or
any endpoint, baseline, archive, library, or credential input through lexical,
canonical, hard-link, or equivalent-parent identity. The SDK-log parent must
be isolated from every other configured service-path parent.

All API, SYS, and the process market-service callbacks enter one capture
function. The callback takes its single-entry gate before touching vendor
memory, sequence state, or the SPSC ring. A losing callback changes only atomic
metrics/fatal state. A winner:

1. copies exactly 23 head bytes with `memcpy`;
2. validates head/message sizes and the service allowlist before subtraction;
3. calls `GetBody()` only for a non-empty body;
4. obtains both clocks;
5. copies metadata, head, body, and a little-endian commit length into the
   preallocated ring;
6. release-publishes the ring cursor;
7. only then publishes the captured ingress sequence.

Publishing the sequence after the ring commit is intentional. A failed ring
push must not claim that a sequence was captured.

## Shadow capture and readiness

Phase 1 uses a simple sequential shadow file with a 64-byte file header and
80-byte record header. The format is native-endian, aligned to eight bytes,
has no checksum/recovery protocol, and is replaced by Phase 2. It handles
short writes and `EINTR`, and clean shutdown requires `fdatasync` and close.
Shadow startup walks every parent from `/` using retained
`openat(..., O_DIRECTORY|O_NOFOLLOW)` descriptors, rejects untrusted or
replaceable ancestors, and requires an effective-UID-owned final parent with
no group/world write permission. A new inode is created owner-only `0600`; an
existing inode must also be singly linked, exactly `0600`, and begin with the
frozen Phase 1 shadow magic before it can be reused. The writer takes a
nonblocking exclusive `flock` before truncation and holds it through close, so
an arbitrary owner-only file, a stopped metrics file, permissive stale files,
and two services targeting the same path all fail closed. The final parent may
not be an SDK-log-marked directory.

The writer manually bounds every relative SYS list offset; it does not call
the vendor `MDLListT` accessors. For the five required core market schemas it
also checks the frozen fixed-body minimum and all 23 dynamic string/list
descriptors, including nested order lists, against the captured body. A
truncated or out-of-range required body remains preserved in the shadow file
but does not set `required_first_seen`. This is structural readiness evidence,
not business-semantic decoding.

A new `LogonResponse` clears all prior connection readiness evidence. READY
requires:

- the latest logon return code is `MDLEC_OK`;
- every required message has a current-connection OK status;
- every required message has subsequently produced a structurally admissible
  record;
- the callback/sink fatal latches remain clear.

This is observational Phase 1 readiness only. It does not create an
authoritative connection or subscription epoch. A failed/replaced logon or
required status after READY causes the service to stop and exit nonzero rather
than continue advertising stale readiness.

Once per second the monitor renders a Prometheus snapshot and submits it to a
dedicated worker; `Submit` performs no filesystem I/O. The worker permits one
in-flight publication and one bounded pending snapshot, replacing an obsolete
pending value with the latest. Sampling therefore remains once per second,
but every intermediate snapshot is not guaranteed to reach disk when
publication is slow.

Default worker construction validates the destination and acquires a
persistent, typed `0600` sidecar whose name is derived from the
case-folded portable basename. It retains both that sidecar's nonblocking
exclusive `flock` and the destination directory fd for the worker lifetime.
This prevents cooperating processes, including ASCII-case variants on
case-insensitive filesystems, from alternately replacing one logical
textfile. The sidecar remains on disk for validated reuse after the lease is
released. The default constructor throws before the worker accepts
submissions if path validation or lease acquisition fails, so service startup
fails closed.

The worker publishes through a same-directory `0600` temporary file,
`fdatasync`, and atomic rename, all relative to the retained directory fd and
original basename. Renaming or replacing the pathname's parent therefore
cannot redirect an active worker into a new directory. The publisher prepends
a frozen metrics type marker; an existing target must be
effective-UID-owned, singly linked, exactly `0600`, nonblockingly lockable,
and already carry that marker. Metrics include build/baseline/config hashes,
callback and ring state, shadow byte/record reconciliation, control counts,
per-required-message status/first-seen evidence, readiness generation, and
fatal state. Token bytes and endpoint addresses are never labels. Periodic
failures are content-redacted, logged at most once per minute, and do not stop
capture. Shutdown submits one final snapshot and drains/joins the worker.
That drain can block in filesystem I/O, so the supervisor hard timeout remains
the ultimate bound. Watchdog pulses begin only after the one-time READY
transition.

Each ingress service requires a pre-provisioned, dedicated SDK-log directory.
Its final parent must be private and owner-controlled and contain
`.l2flow-sdk-log-directory-v1` as a non-symlink, singly linked regular file
with exact mode `0444` and exact contents
`l2flow-sdk-log-directory-v1\n`. Startup retains the directory fd and passes
`/proc/self/fd/N/<basename>` to `EnableLog`, preventing ancestor
rename/replacement from redirecting SDK-created suffix files. It also retains
a nonblocking exclusive `flock` on the marker for the lease lifetime, so a
second cooperating ingress cannot concurrently use that directory. Shadow
and metrics writers reject any directory containing the reserved marker.

The marker is an operator-declared trust boundary, not a sandbox against
mutually hostile processes sharing one UID. Deployment must give each service
its own marked directory and use service-UID or mount isolation when same-UID
prefix collisions would otherwise be possible.

## Shutdown safety

Normal shutdown is:

```text
gate new callbacks
-> IOManager::Shutdown
-> prove callback quiescence
-> stop and drain shadow writer
-> fdatasync/close and join
-> release Subscriber
-> release IOManager
-> submit final metrics snapshot
-> drain and join metrics worker
```

The handler/router/ring outlive both SDK objects. If opaque `Shutdown`
returns by throwing, the quiescence deadline expires, or final SDK reference
counts cannot prove this ordering, the process terminates without unwinding
callback-owned storage. A vendor `Connect`/`Shutdown` call that never returns
cannot be bounded safely inside this process; the supervisor hard timeout and
real-endpoint boundedness test remain deployment requirements.

## Deliberate Phase 1 limits

- No portable Raw WAL, CRC, durable journal, crash recovery, replay, decoder,
  canonical log, or authoritative epoch is claimed.
- A real endpoint must still prove bounded Connect/Shutdown behavior.
- The design's eight-hour, four-stream, p99/p99.9, restart, and real low-rate
  shadow acceptance requires credentials, permissions, reviewed network
  endpoints, and the target deployment host.
