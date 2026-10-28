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
  - Automatic connection & reconnection with exponential backoff
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
    └── reconnect_probe.cc  # Reconnect / leak probe suite
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
| `bigmsg`      | Inbound PUBLISH with 1/2/3 byte remaining lengths parse correctly.     |
| `bigpub`      | Large publishes into a stalled peer must not corrupt the stream.       |

Scenarios listen on `127.0.0.1` at `18840 + slot`; export
`LITEMQTT_PROBE_PORT_BASE` to move the whole block. Exit code `0` means the
observed behaviour matched expectations.

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

    helper->on_message([](std::string topic, std::string payload,
                          uint8_t qos, uint16_t packet_id) {
        // handle incoming message
    });

    helper->subscribe("hello/world", 1);
    helper->start();

    // Application runs independently, reconnection handled automatically
    std::this_thread::sleep_for(std::chrono::seconds(300));
    helper->stop();
}
```

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
