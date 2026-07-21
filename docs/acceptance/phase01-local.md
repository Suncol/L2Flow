# Phase 0–1 local acceptance record

Date: 2026-07-18  
Host scope: local Linux x86-64 development container

## Completed local evidence

- C++20 production and C++17 mock targets compile with strict warnings as
  errors.
- The normal `RelWithDebInfo` suite has 36 tests. 35 pass; the sole failure is
  `test_phase0_baseline`, the deliberately closed Phase 0 artifact gate
  described below. Its five failed assertions are exactly one direct ELF-size
  mismatch and four complete-gate/runtime expectations: probe attempted, full
  baseline/archive/library report passed, wrong-version result passed, and
  current-version result passed. The uploaded archive now passes its approved
  hash check. The full report records only the workspace library hash as
  `<unavailable>` because that copy is rejected by the exact-size gate before
  copying or hashing.
- The ASan+UBSan build passes all 35 sanitizer-applicable tests. LeakSanitizer
  alone is disabled because this ptrace-based container cannot run it
  reliably; AddressSanitizer and UndefinedBehaviorSanitizer remain enabled.
- The TSan build compiles every configured target. This container cannot start
  even the smallest TSan test because the runtime exits with
  `unexpected memory mapping`; no TSan runtime pass is claimed.
- `CALLBACK-001` includes one producer and one concurrent SPSC consumer for
  exactly 10,000,000 zero-body callbacks. It proves exact callback count,
  ingress sequence, vendor bytes, ring entries, and no body access or fatal.
- ABI/message contracts, callback validation and exception containment,
  reentry rejection, SPSC wrap/threshold/overflow/property/concurrency cases,
  clock/config math, credentials, systemd datagrams, shadow short-write/error
  paths, SYS bounds, readiness generations, exact callback/sink
  reconciliation, and ordered SDK lifecycle tests pass.
- Endpoint connection behavior is loaded only from a versioned, hash-pinned
  strict JSON contract. The loader uses one `O_NOFOLLOW` regular-file
  descriptor for read, hash, and parse; tests cover malformed schemas,
  symlinks, size bounds, hash/kind mismatch, and 2,000 concurrent path
  replacements. The service rejects every individual endpoint CLI override.
- The compiled baseline JSON is checked by exact-size capture into one sealed
  snapshot and byte comparison on that retained descriptor; reporting does
  not reopen the mutable pathname. A passing report uses the SHA-256 of those
  byte-exact compiled contents; a failed capture reports the actual value as
  unavailable rather than hashing a second path identity. Archive hashing
  opens one `O_NOFOLLOW|O_NONBLOCK` regular-file descriptor and enforces a
  1 GiB operational cap before reading. That cap bounds work and is not part
  of the approved archive identity.
- The production library path is copied byte-for-byte into a close-on-exec
  memfd and sealed against writes, growth, shrinking, and further seal
  changes. The approved byte size is required before memfd allocation or
  copying. Service startup performs the full artifact preflight on sealed
  snapshot A. The production loader then captures fresh sealed snapshot B,
  repeats the library component hash/ELF/ABI/runtime gate on B, and uses B for
  final `dlopen`; each gate therefore checks and loads one immutable byte
  identity, without claiming A and B are the same capture. Self-contained
  tests prove exact copying, source mutation/unlink independence, all four
  seals, caller-fd offset preservation, symlink/FIFO/non-regular rejection,
  path-private errors, and rejection of an ordinary fd passed to the sealed-fd
  continuation. The unapproved workspace library is never executed.
- The uploaded `mdl_sdk_2_13_234.tar.gz` matches the approved archive digest.
  Its approved library member was independently streamed and then extracted
  into a new private `/tmp` directory without replacing the damaged workspace
  copy. The full `mdl_abi_preflight` was run twice, including once from an
  isolated working directory, under a 120-second process timeout. Both runs
  produced the same report bytes: all 300 checks passed,
  `runtime_probe_attempted=true`, wrong version returned null, current version
  created and safely shut down/released, and `dlclose` succeeded. The report
  is saved as
  [`phase0-preflight-20260718.json`](phase0-preflight-20260718.json); its
  SHA-256 is
  `750ec2c5e123ab77b46caac0503b446d039ee3904c839619ce48f2985826b943`.
- Service-path tests cover lexical, canonical, hard-link, and equivalent-parent
  alias rejection, portable basenames, missing parents, no-symlink dirfd
  ancestry walks, unsafe ancestor/final-parent permissions, directory-rename
  anchoring, and exact SDK-log directory marker validation. The SDK-log lease
  retains an exclusive nonblocking `flock` on that marker, so a second
  cooperating service cannot concurrently lease the same directory. Existing
  shadow and metrics targets require their own frozen type markers, and both
  writers reject an SDK-log-marked directory, preventing cross-type reuse.
- Required-body readiness tests exercise all 23 dynamic string/list
  descriptors in the five required core schemas with valid, truncated,
  out-of-body, and empty ranges. Structurally invalid records remain in the
  shadow capture but cannot satisfy first-seen readiness.
- Prometheus text is generated without token or endpoint-address labels and is
  submitted once per second by the monitor to a dedicated bounded,
  latest-pending-wins worker; monitor and SDK callback threads perform no
  metrics filesystem I/O. Tests cover the one-in-flight/one-pending bound,
  backend failure and exception containment, content-free errors, final
  shutdown drain, the frozen metrics type marker, the complete-file one-MiB
  bound, exact `0600` mode, old/new reader consistency, failure cleanup,
  persistent typed sidecar ownership leases, cross-process/case-variant
  collision rejection, retained-dirfd publication after parent-directory
  rename/replacement, build/config/shadow/readiness fields, and the pure
  READY/status/watchdog policy. The production worker holds the sidecar
  `flock` and destination dirfd for its lifetime; every replacement is
  relative to that retained directory. Periodic publication failures are
  logged at most once per minute and do not stop capture.
- Four independent ingress executables compile and their dynamic sections are
  checked to contain no `libmdl_api.so` dependency. A separate minimal vendor
  target compiles the reviewed factory, `CreateSubscriber`, configuration,
  subscription, `Connect`, and `Shutdown` surface and is checked by `readelf`
  to require `libmdl_api.so`; it is not executed before the full artifact gate.

Build-manifest SHA-256 values for this run:

```text
normal (RelWithDebInfo)  3f0708417b5307e6076c60cc25a9b30257d3bd8646ed67be09f50293ab079c87
ASan+UBSan (Debug)       cbc85d59ba80de63addd4439839f79394f37061b2b9adb1f210e459ed9137ce9
TSan (Debug)             65ccac8a8c2ceb3d66651f3829916dd8c49c6617ab4e3dab4d7f621afd3c0520
baseline                 cff17ade6228585aa52763e5076dda67293071618706fc689917b0585ffceaf1
```

## Workspace default-path gate remains deliberately closed

The immutable approved library identity remains:

```text
size    242357680
sha256  09bd58282d6f758bfb737b628f5c51daa591a60f31d4081992679fcbc2e2cfc5
```

The current workspace copy is:

```text
size    242356792
sha256  e80d1891ccf6fd87845d176bc40cced0c33d589e56a2f2ef05560e6efc6d7d79
```

It is 888 bytes shorter. The uploaded SDK archive is now approved:

```text
path             mdl_sdk_2_13_234.tar.gz
expected sha256  23830887091d35875c653d97a4874f27f04b4cc36a69de37a952510d0701cc71
actual sha256    23830887091d35875c653d97a4874f27f04b4cc36a69de37a952510d0701cc71
```

The default-path CTest still correctly skips the runtime factory probe because
it points at the short workspace library. No unapproved vendor library is
executed, and the approved constants must not be updated to accept that file.
The true full Phase 0 gate is green when pointed at the approved library
extracted from the archive; a production service must use that approved copy
or an equivalently verified durable restoration, never the short workspace
copy.

## External acceptance still required

The following design exit conditions cannot be established locally:

- real login and required-subscription OK for all four reviewed endpoint
  contracts;
- first real required record on all four streams;
- eight-hour shadow with no callback reentry or ring overflow;
- callback p99/p99.9 SLO on the target host;
- real SDK `Connect`/`Shutdown` boundedness and repeated restart behavior;
- target-host dependency/glibc/libstdc++ compatibility;
- production Prometheus collector permissions, shadow-directory ownership,
  and supervisor hard timeouts;
- provisioning one private, correctly marked SDK-log directory per ingress
  service;
- service-UID or mount isolation sufficient to prevent mutually writable
  same-UID processes from colliding on SDK-created log suffixes.

These are explicit deployment acceptance items, not simulated successes.
