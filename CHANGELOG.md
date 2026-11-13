# Changelog

All notable changes to this project are documented here.  The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

LiteMQTT is header only, so a version is a set of headers: upgrading means
replacing them.

## [Unreleased]

### Fixed

- **A packet can no longer be read past the remaining-length buffer.** When the
  length field used a fourth continuation byte, `read_remaining_length_step()`
  indexed `byte_buffer[4]` before checking the bound, reading one byte past a
  `std::array<uint8_t, 4>`.  A peer that sent five continuation bytes in a row
  drove the read out of bounds; with MSVC's iterator debugging it aborts, and
  without it the value is whatever follows the array in the connection object.
  The bound is now checked before the byte is read.

- **`acquire_packet_id()` can report exhaustion.** The search bounded itself by
  comparing the cursor against the id it started after, but that id is one that
  was already handed out, so the walk wrapped around and returned an id that may
  still be free.  A session with all 65535 packet ids in use silently reused one
  instead of noticing.  It now counts the ids it has probed and returns 0 (and
  closes the session) when none is free.

- **`parse()` rejects malformed packets instead of returning half a struct.**
  Every parser ignored the `buffer_reader` return values and never compared the
  declared remaining length against the bytes present.  A two-byte CONNACK, for
  example, produced `return_code == 0`, indistinguishable from a successful
  connect.  `parse()` now takes the target by reference and returns `bool`;
  truncated, padded and wrongly-typed packets are rejected, as is a PUBLISH with
  the reserved QoS 3.  Malformed packets reaching `mqtt_client` are logged as
  protocol errors and close the session.

- **SUBACK length mismatch is caught with or without a callback.**
  `handle_suback()` returned early when the caller passed no callback, before
  checking that the broker answered every filter.  `mqtt_helper` restores
  subscriptions with a null callback, so the check was skipped on every
  reconnect.  The count check now runs first.

- **`async_subscribe()` refuses without a session.** It queued the SUBSCRIBE
  whenever the client was not closed, including while connecting and after a
  failed handshake, leaving `pending_subscribes_` entries nothing would resolve.
  It now reports failure through the callback, like `async_publish()`.

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

- `connection::set_max_packet_size()` / `max_packet_size()`, forwarded by
  `mqtt_client` and `mqtt_helper` so the cap is reachable from the high-level API
  and survives the client a reconnect rebuilds.  A peer controls the remaining
  length and the body buffer is allocated at its full size, so a declared length
  of 268435455 asked for 256 MiB before a payload byte arrived.  The default cap
  is `connection::kDefaultMaxPacketSize` (1 MiB) and a larger packet is reported
  as `asio::error::message_size`.  **Behaviour change**: a client that exchanges
  messages above 1 MiB has to raise the cap, or set it to 0 to disable the check
  -- which hands the allocation size back to the peer.
- `mqtt_helper::set_max_reconnect_delay()`, so the exponential backoff ceiling is
  configurable instead of a hardcoded 30 s.
- `mqtt_client::set_retry_interval_for_test()`,
  `set_max_retransmits_for_test()` and `debug_set_next_packet_id()`, to drive the
  retransmit timers and the id allocator from a test.  Test hooks, not part of
  the supported API.
- `buffer_writer::kMaxRemainingLength`.

### Changed

- `buffer_writer::write_string()` and `write_remaining_length()` throw
  `std::length_error` instead of emitting a frame that contradicts its own length
  prefix: a string over 65535 bytes was truncated to a prefix of 0, and a length
  over 268435455 produced a five-byte field no decoder accepts.  `serialize()`
  surfaces this, so an oversized topic, payload or client id now throws where it
  used to build a corrupt packet.
- `connect_packet::parse()` enforces the protocol name, the protocol level and
  MQTT 3.1.1 section 3.1.2.9 (a password flag without a user name is invalid).
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
The suite went from 55 cases in 5 files to 116 in 7.

- `test/robustness_test.cc` feeds every parser truncated, padded, wrongly-typed
  and over-long input, plus the CONNECT edge cases (wrong protocol name or
  level, password without user name).
- `test/connection_test.cc` drives inbound framing errors over a real socket:
  reserved packet types, a five-byte remaining length, and a declared body above
  the new cap (which previously allocated 256 MiB and hung).
- `test/client_test.cc` covers the CONNACK accept/refuse paths, malformed
  packets, the SUBACK count check with and without a callback, the subscribe
  guards, packet id exhaustion and inbound PUBLISH rejection.
- `test/helper_test.cc` adds publish-after-stop, backoff growth to its ceiling,
  the connect timeout, and both keepalive directions.
- `test/codec_test.cc` adds the encoder boundaries.
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
