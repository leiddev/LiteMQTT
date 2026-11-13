# Changelog

All notable changes to this project are documented here.  The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

LiteMQTT is header only, so a version is a set of headers: upgrading means
replacing them.

## [Unreleased]

### Fixed

- **QoS 2 publishes now complete.** `handle_pubrec()` moved the publish into the
  PUBREL half of the handshake but left the entry in `pending_publishes_`, which
  carried no callback.  `handle_pubcomp()` therefore found nothing to report and
  a QoS 2 publish whose PUBREC and PUBCOMP both arrived **never invoked its
  publish callback**.  Callers waiting on that callback, or looping on a
  `pending` flag it was meant to clear, stalled.  The topic, QoS and callback now
  travel with the flow, and a completed handshake reports success exactly once.

- **A duplicate PUBREC no longer restarts the QoS 2 flow.** Because the publish
  stayed in `pending_publishes_`, a second PUBREC for the same packet id
  re-entered the "outgoing publish" branch, reset the retry counter to 0 and sent
  another PUBREL.  A broker that repeats PUBREC kept the message alive forever:
  the give-up limit could never be reached.  The `pending_pubrecs_` lookup now
  happens first, and such a duplicate is ignored.

- **QoS 2 retransmission is bounded.** `send_pubrel()` armed the retry timer
  twice -- once before the write and once in its completion handler -- so the
  second call silently restarted the window and every PUBREL retry waited a full
  interval from the previous write rather than from when the timer fired.

- **A failed QoS 2 publish is reported.** When the PUBREL retries were exhausted
  the session was closed without failing the pending publish, so its callback
  never ran.  `notify_closed_once()` now fails pending `pending_pubrecs_` entries
  the same way it fails pending QoS 1 publishes.

### Added

- `mqtt_client::set_retry_interval_for_test()` and
  `set_max_retransmits_for_test()`, to drive the QoS 1/2 retransmit timers from
  a test.  The defaults are unchanged (5 s, 3 attempts).  These are test hooks,
  not part of the supported API.

### Changed

- The retransmit interval and retry limit are members rather than hardcoded
  literals; the unused `kPublishRetryInterval` constant is gone.  Behaviour with
  the default configuration is identical.

### Documentation

- `examples/`: the demo moved from `main.cc` to `examples/demo.cc`, with its own
  README covering the options and behaviour.  No code change; the executable is
  still `LiteMQTTDemo`.
- The Windows/MSVC build recipe is documented: configure from a `vcvars64`
  environment, and why `LITEMQTT_FETCH_GTEST` and `LITEMQTT_REQUIRE_TESTS` both
  matter there.
- `docs/测试补充清单.md` records what the tests cover and what they do not.

### Tests

Not part of the released headers, listed because they back the fixes above.

- `test/qos_test.cc` drives a real `mqtt_client` over loopback: QoS 1
  retransmission with the DUP flag and a stable packet id, giving up after the
  configured attempts, an acknowledged publish never being retransmitted, a
  duplicate PUBACK being ignored, the full QoS 2 handshake, PUBREL retransmission
  up to the limit, and a duplicate PUBREC not restarting a flow.
- `test/loopback_broker.hpp` is a scripted MQTT peer that records what it
  receives and sends only what the test queues.  Its teardown used to hang when
  the client never connected.

## [1.0.0] - 2026-11-06

First tagged release.

### Added

- `mqtt_helper`: a high-level client with automatic reconnection -- unlimited or
  capped attempts, exponential or fixed backoff, re-armable after giving up --
  and subscription restore across reconnects.
- `mqtt_client`: the asynchronous `mqtt_client` API on top of standalone Asio,
  with QoS 0/1/2 publish and subscribe, keepalive, packet id allocation and
  bounded pending-publish backpressure.
- Pluggable logging: a process-wide handler plus per-object channels, severity
  thresholds, and source locations on every record.
- `LiteMQTT::litemqtt` CMake INTERFACE target carrying the include directories,
  C++14 and the bundled Asio, with install and `find_package` rules.
- `reconnect_probe` and `reconnect_stress`, two loopback tools for exercising
  reconnection behaviour that unit tests cannot observe.

[Unreleased]: https://github.com/leiddev/LiteMQTT/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/leiddev/LiteMQTT/releases/tag/v1.0.0
