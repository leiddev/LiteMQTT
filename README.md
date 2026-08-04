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
│       └── mqtt_helper.hpp # High-level helper with auto-reconnect
└── test/
    └── codec_test.cc       # GoogleTest unit tests
```

## Build

Configure with CMake (e.g. with Ninja) and build:

```bash
cmake -B build -G Ninja
cmake --build build
```

The build produces:

- `LiteMQTTDemo` – the demo client (`main.cc`)
- `codec_test` – unit tests (if GoogleTest is available)

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