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
- Variable-length encoding/decoding for Remaining Length
- Small, dependency-light footprint

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
│       └── error.hpp
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

Press Ctrl-C to disconnect and exit.

## Usage Example

```cpp
#include <asio.hpp>
#include <litemqtt/litemqtt.hpp>

int main() {
    asio::io_context io;
    auto client = std::make_shared<litemqtt::mqtt_client>(io);

    client->set_client_id("my_client");
    client->set_keep_alive(60);
    client->set_clean_session(true);

    client->on_connect([client](bool ok, uint8_t rc) {
        if (ok) {
            client->async_subscribe("hello/world");
            client->async_publish("hello/world", "hi");
        }
    });

    client->on_message([](std::string topic, std::string payload) {
        // handle incoming message
    });

    client->async_connect("127.0.0.1", 1883, [](bool, uint8_t) {});
    io.run();
}
```