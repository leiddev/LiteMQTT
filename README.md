# LiteMQTT

A lightweight, header-only MQTT v3.1.1 client library written in C++14, powered by Asio.

## Features

- Asynchronous API built on Asio
- Header-only design
- MQTT v3.1.1 protocol support
  - CONNECT / CONNACK
  - PUBLISH
  - SUBSCRIBE / SUBACK
  - UNSUBSCRIBE / UNSUBACK
  - PINGREQ / PINGRESP
  - DISCONNECT
- Built-in Keep-Alive mechanism with timeout detection
- Username/Password authentication support
- Variable-length encoding/decoding for Remaining Length
- Small, dependency-light footprint
- **Pluggable logging** - install a log callback to forward all library messages
  to your own logging backend (see [Logging](#logging))
- **mqtt_helper** - High-level convenience wrapper with:
  - Automatic connection & reconnection: unlimited or capped attempts,
    exponential or fixed backoff, re-armable after giving up
  - Thread-safe io_context management
  - Subscription persistence across reconnects
  - Simple blocking-style API for easy integration

## Project Layout

```
LiteMQTT/
├── CMakeLists.txt          # Top-level build file
├── main.cc                 # Demo application
├── 3rdparty/
│   ├── asio/               # Vendored standalone Asio
│   └── asio.cmake          # Asio CMake module
├── inc/
│   └── litemqtt/
│       ├── litemqtt.hpp    # Umbrella header
│       ├── client.hpp      # High-level mqtt_client API
│       ├── connection.hpp  # TCP transport & framing
│       ├── packets.hpp     # MQTT control packet structs
│       ├── packet_type.hpp # MQTT packet type enum
│       ├── buffer_writer.hpp
│       ├── buffer_reader.hpp
│       ├── error.hpp
│       ├── log.hpp         # Log levels, callback interface & default sink
│       └── mqtt_helper.hpp # High-level helper with auto-reconnect
├── test/
│   ├── codec_test.cc       # GoogleTest unit tests
│   ├── client_test.cc
│   ├── connection_test.cc  # Framing, write serialization
│   ├── helper_test.cc      # Lifetime, stop()/start(), backoff
│   └── log_test.cc
└── tools/
    ├── reconnect_probe.cc   # Reconnect / leak probe suite
    └── reconnect_stress.cc  # Publish-while-reconnecting stress harness
```

## Build

Configure with CMake (e.g. with Ninja) and build:

```bash
cmake -B build -G Ninja
cmake --build build
```

The build produces:

- `LiteMQTTDemo` – the demo client (`main.cc`)
- `reconnect_probe` – reconnect / leak probe suite (`tools/`)
- `reconnect_stress` – publish-while-reconnecting stress harness (`tools/`)
- `codec_test`, `client_test`, `connection_test`, `helper_test`, `log_test` – unit tests
  (if GoogleTest is available)

## Running

The demo client connects to a local broker, subscribes to a topic and publishes one message:

```bash
./LiteMQTTDemo --host 127.0.0.1 --port 1883 \
               --id litemqtt_demo \
               --subscribe litemqtt/demo \
               --publish litemqtt/demo \
               --payload "Hello from LiteMQTT!"
```

With authentication:

```bash
./LiteMQTTDemo --host 127.0.0.1 --port 1883 \
               --username myuser --password mypass \
               --subscribe litemqtt/demo \
               --publish litemqtt/demo \
               --payload "Hello from LiteMQTT!"
```

Press Ctrl-C to disconnect and exit.

## Reconnect Probes

`reconnect_probe` exercises the failure modes that unit tests cannot observe: it
spins up a tiny broker on loopback, breaks the link in a specific way and prints
the numbers that decide pass/fail.

```bash
./reconnect_probe --help      # list scenarios
./reconnect_probe framing     # one scenario
./reconnect_probe all         # everything, with a PASS/FAIL summary
```

| Scenario      | What it checks                                                        |
|---------------|-----------------------------------------------------------------------|
| `reconnect`   | Broker drops the link repeatedly; the helper must come back.           |
| `leak`        | The helper must be destroyed after `stop()` and releasing it.          |
| `leak_nostop` | Dropping the last owner **without** `stop()` must not leak the thread. |
| `stoplag`     | `stop()` must not block while a long backoff timer is armed.           |
| `restart`     | `start()` after `stop()` must connect again.                           |
| `selfstop`    | Calling `stop()` from the io thread (inside a callback) must be safe.  |
| `badcreds`    | A CONNECT rejected by the broker must close the session immediately.   |
| `framing`     | Many large writes plus broker-side QoS 1 pushes must stay in sync.     |
| `backoff`     | The io thread must stay responsive during the backoff wait.            |
| `giveup`      | A later `start()` must reset the attempt counter after giving up.       |
| `rearm`       | `rearm_reconnect()` resumes retrying after the limit was hit.          |
| `fixed_delay` | `set_reconnect_config(-1, 3000)` retries forever, every 3 s.           |
| `bigmsg`      | Inbound PUBLISH with 1/2/3 byte remaining lengths parse correctly.     |
| `bigpub`      | Large publishes into a stalled peer must not corrupt the stream.       |

Scenarios listen on `127.0.0.1` at `18840 + slot`; export
`LITEMQTT_PROBE_PORT_BASE` to move the whole block. Exit code `0` means the
observed behaviour matched expectations.

## Reconnect Stress Harness

`reconnect_stress` answers a different question than the probes: *does anything
leak while the client keeps publishing through a link that is down, flapping or
half-dead?* It starts a chaos broker on loopback, drives the link through a
scripted schedule and publishes QoS 1 messages at a fixed rate the whole time.

```bash
./reconnect_stress --help                 # list scenarios and options
./reconnect_stress --scenario flap     --duration 30
./reconnect_stress --scenario weak     --duration 120 --rate 200
./reconnect_stress --scenario soak     --duration 300
./reconnect_stress --scenario mixed    --duration 120
```

| Scenario    | Link behaviour                                                        |
|-------------|-----------------------------------------------------------------------|
| `flap`      | The broker drops every session after ~80 ms, forever.                  |
| `long`      | One long outage, then recovery.                                       |
| `blackhole` | The listener accepts and then goes silent (no CONNACK, no reads).      |
| `weak`      | Slow reads plus short outages — a weak link, not a dead one.           |
| `mixed`     | All of the above in one schedule.                                      |
| `soak`      | Same schedule as `mixed`; use it for long endurance runs.              |

The broker listens on `127.0.0.1` and asks the OS for a free port, so two runs
can never share traffic — the client is always pointed at its own broker. Pin
a port with `--port N` when you want to watch the wire (tcpdump) or reproduce
a specific setup. If the port cannot be bound the tool exits with status 2
instead of quietly measuring a listener owned by another process, and losing
the port in the middle of a run (an outage closes the listener, so a second
run can grab it) fails the run rather than reporting numbers from a broker
that never saw the traffic.

Give it at least ~30 s: `weak` and `mixed` start with a few seconds of weak but
working link, so a very short run can finish without ever reaching an outage —
and then fails the "link broke and came back" check on its own merits.

Every publish must be answered exactly once, and the summary checks that the
broker saw no framing or ordering errors, that the link really did break and
come back, that `stop()` returns immediately and that descriptors, threads and
RSS did not grow. The three resource counters are read from `/proc`, so they
are only meaningful on Linux (elsewhere those checks pass trivially). Run it
under AddressSanitizer for a leak check that goes beyond those counters (RSS is
meaningless there, so relax it):

```bash
g++ -std=c++14 -g -O1 -fsanitize=address -I inc -I 3rdparty/asio/include \
    -pthread tools/reconnect_stress.cc -o reconnect_stress_asan
ASAN_OPTIONS=detect_leaks=1 ./reconnect_stress_asan --scenario mixed \
    --duration 30 --rss-limit-mb 4096
```

## Usage Example

### Using `mqtt_client` (Low-level async API)

```cpp
#include <asio.hpp>
#include <litemqtt/litemqtt.hpp>

int main() {
    asio::io_context io;
    auto client = std::make_shared<litemqtt::mqtt_client>(io);

    client->set_client_id("my_client");
    client->set_keep_alive(60);
    client->set_clean_session(true);
    client->set_username("myuser");
    client->set_password("mypass");

    client->on_connect([client](bool ok, uint8_t rc) {
        if (ok) {
            client->async_subscribe("hello/world");
            client->async_publish("hello/world", "hi");
        }
    });

    client->on_message([](std::string topic, std::string payload, uint8_t qos) {
        // handle incoming message
    });

    client->async_connect("127.0.0.1", 1883, [](bool, uint8_t) {});
    io.run();
}
```

### Using `mqtt_helper` (High-level API with auto-reconnect)

```cpp
#include <litemqtt/mqtt_helper.hpp>

int main() {
    auto helper = std::make_shared<litemqtt::mqtt_helper>(
        "127.0.0.1", 1883, "my_client");

    helper->set_credentials("myuser", "mypass");
    helper->set_keep_alive(60);
    helper->set_reconnect_config(10, 1000);  // max 10 attempts, 1s base delay
    // helper->set_reconnect_config(-1, 3000);  // or: retry forever, every 3 s

    helper->on_message([](std::string topic, std::string payload,
                          uint8_t qos, uint16_t packet_id) {
        // handle incoming message
    });

    helper->subscribe("hello/world", 1);
    helper->start();

    // Application runs independently, reconnection handled automatically
    std::this_thread::sleep_for(std::chrono::seconds(300));

    // With a finite max_attempts the helper eventually gives up (it logs a warn
    // and is_reconnect_armed() turns false) but keeps running, so the session can
    // be revived without tearing the io thread down:
    if (!helper->is_reconnect_armed()) {
        helper->rearm_reconnect();  // resets the counter, retries immediately
    }

    helper->stop();
}
```

`set_reconnect_config(max_attempts, base_delay_ms)`:

| `max_attempts` | Behaviour                                                                 |
|----------------|---------------------------------------------------------------------------|
| `-1`           | Retry forever, always waiting `base_delay_ms` (no exponential growth).     |
| `>= 0`         | Give up after that many consecutive failures; the delay grows as `base_delay_ms * 2^attempt`, capped at 30 s. A successful connect resets the counter. |

After giving up, `is_reconnect_armed()` returns `false` and `rearm_reconnect()`
puts the helper back to work (returns `false` if it is not running, in which case
`start()` is what you need). Both are safe to call from any thread.

Every attempt is bounded by a connect timeout that covers TCP connect *and* the
CONNACK wait, so a blackholed peer cannot stall the session forever:

```cpp
helper->set_connect_timeout(10000);  // ms, default 10000; 0 disables the timeout
```

A timed-out attempt is treated like any other failure: `on_connect(false, 255)`
fires, the socket is closed and the reconnect policy decides what happens next.

While a session is not connected, `publish()` does not queue and does not touch
the socket — it reports `callback(false, topic, qos, 0)` immediately. That keeps
a publish from a timer from tearing down a connect attempt that is in progress;
re-publish from your `on_connect(true)` handler (or retry on the failure
callback) once the link is back.

## Delivery Guarantees

The publish window lives in memory only — nothing is persisted — and the
callback contract is deliberately narrow. It is the same for `mqtt_client` and
`mqtt_helper`:

| QoS | `callback(true, ...)` means                                  | Retry inside the session |
|-----|--------------------------------------------------------------|--------------------------|
| 0   | the PUBLISH frame was handed to the socket                    | none (fire and forget)   |
| 1   | the broker's PUBACK for this packet id arrived                | 5 s, up to 3 attempts, `DUP = 1` on retries |
| 2   | the broker's PUBCOMP arrived (PUBLISH → PUBREC → PUBREL → PUBCOMP) | 5 s / 3 attempts for PUBLISH → PUBREC as above; 5 s / 3 attempts for PUBREL → PUBCOMP |

Three rules follow from that, and they are intentional:

- **A disconnect fails everything still in flight.** `notify_closed_once()`
  reports `callback(false, topic, qos, packet_id)` for every unacknowledged
  publish (and every un-SUBACKed subscribe) and drops it. Nothing is re-sent
  after the next `on_connect(true)`.
- **QoS 1 retries at most three times, then gives up.** No PUBACK within 5 s
  re-sends the frame with `DUP = 1`; after the third attempt the publish is
  reported as failed even though the broker may in fact have received it.
  A QoS 2 message that never gets its PUBCOMP aborts the session instead
  (which, under `mqtt_helper`, means `on_connect(false, 255)` and a reconnect).
- **There is no offline queue.** Publishing while the session is down
  (connecting, waiting for CONNACK, or between reconnects) reports
  `callback(false, topic, qos, 0)` immediately — it is never buffered, and it
  does not disturb the connect attempt in progress. The in-flight window is
  bounded at 1000 publishes (`kMaxPendingPublishes`); when it is full the new
  publish is rejected with `false` and a warning is logged.

Republishing is the application's responsibility: re-send from
`on_connect(true)` whatever was in flight, or retry from the failure callback.
Keep that path idempotent — a `DUP = 1` frame the broker did receive can still
be delivered twice. Subscriptions are per session as well, so re-issue them
after a reconnect; `mqtt_helper` does that for you for everything registered
before `start()`.

If you need at-least-once across reconnects, keep the queue *above* the
library. `tools/reconnect_stress.cc` is a worked example of the bookkeeping:
it counts every publish and fails the run if one is never reported back (and
if any is reported twice).

## Logging

The library never writes to `std::cout` / `std::cerr` directly any more: every
message goes through the sink installed with `litemqtt::set_log_handler()`, so it
can be forwarded to any logging backend (spdlog, glog, Qt, your own logger, ...).

```cpp
#include <spdlog/spdlog.h>
#include <litemqtt/log.hpp>

litemqtt::set_log_handler([](const litemqtt::log_record& record) {
    switch (record.level) {
        case litemqtt::log_level::trace:
        case litemqtt::log_level::debug: spdlog::debug("{}", record.message); break;
        case litemqtt::log_level::info:  spdlog::info("{}", record.message);  break;
        case litemqtt::log_level::warn:  spdlog::warn("{}", record.message);  break;
        case litemqtt::log_level::error: spdlog::error("{}", record.message); break;
        case litemqtt::log_level::off:   break;
    }
});

// Optional: discard everything below `info` (default threshold: log_level::trace).
litemqtt::set_log_level(litemqtt::log_level::info);
```

If you only need level + message, use the adapter:

```cpp
litemqtt::set_log_handler(litemqtt::make_log_handler(
    [](litemqtt::log_level level, const std::string& message) {
        my_logger::write(litemqtt::to_string(level), message);
    }));
```

Notes:

- The logger is process-wide; call `set_log_handler()` once during startup
  (before creating clients is recommended, but it can be changed at any time).
  Pass a default-constructed `litemqtt::log_handler{}` to restore the built-in
  sink (`warn`/`error` → `std::cerr`, the rest → `std::cout`).
- The handler is invoked from the thread that emitted the event — usually the
  Asio `io_context` thread — so it must be thread-safe. It is always called
  *without* the library holding any lock, so it may reconfigure the logger.
  Exceptions thrown by the handler are swallowed.
- `litemqtt::log_record` carries `level`, `file`, `line` and `message`
  (no trailing newline). Source location is captured by the internal macros.
- `litemqtt::log_enabled(level)` can be used to skip building expensive messages.

### Per-instance handlers

The process wide sink above is usually what you want when the whole application
shares one logging backend. If you need to tag / route the records of individual
connections differently (multiple brokers, per-tenant loggers, tests, ...), install
a handler on the object instead — it wins over the global one:

```cpp
// Per client (mqtt_client) ...
client->set_log_handler([id](const litemqtt::log_record& record) {
    my_logger::write(id, record.level, record.message);
});

// ... or per helper (mqtt_helper): applies to the helper and to the mqtt_client
// it owns, so transport level records are routed to the same backend.
helper->set_log_handler([id](const litemqtt::log_record& record) { /* ... */ });
helper->set_log_handler(litemqtt::log_handler{});  // detach -> global sink again
```

Resolution order for every record:

1. the handler of the emitting object (`mqtt_client` / `mqtt_helper`), if installed;
2. otherwise the process wide handler from `litemqtt::set_log_handler()`;
3. otherwise the built-in sink (`warn`/`error` → `std::cerr`, rest → `std::cout`).

`has_log_handler()` / `get_log_handler()` let you query the installed sink;
`set_log_handler()` may be called at any time, from any thread, including while
the client is running. The severity threshold from `litemqtt::set_log_level()`
stays global and applies to per-instance handlers too. The same mechanism is
available for your own classes through `litemqtt::detail::log_channel` +
`LITEMQTT_LOG_TO(channel, level) << ...`.

### What gets logged

| Level  | Where                     | Event                                                        |
|--------|---------------------------|--------------------------------------------------------------|
| error  | `client.hpp`              | TCP connect failure, protocol error (invalid PUBLISH QoS), invalid QoS passed to `async_publish` / `async_subscribe`, packet-id exhaustion |
| warn   | `client.hpp`              | `async_publish` backpressure limit reached                   |
| error  | `mqtt_helper.hpp`         | `io_context` exception, connection failure, null client, reconnect attempts exhausted |
| warn   | `mqtt_helper.hpp`         | giving up after max reconnect attempts                        |
| info   | `mqtt_helper.hpp`         | connection opened/closed, connected to broker, initiating/reconnecting |
| debug  | `mqtt_helper.hpp`         | `io_context` thread exit, connection callback details         |
