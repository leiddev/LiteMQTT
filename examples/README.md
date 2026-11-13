# Examples

## demo

A complete `mqtt_helper` client: it connects to a broker, subscribes to a
topic, and publishes a message in a loop until Ctrl-C.

It is the smallest program in the repository that exercises the high-level API
end to end, so it is the one to read first.  All of the following are done with
`mqtt_helper` rather than `mqtt_client`: automatic reconnection, subscription
restore after a reconnect, and per-instance logging.

### Build

From the top of the repository:

```bash
cmake -B build -G Ninja
cmake --build build
```

The binary is `build/LiteMQTTDemo` (`build\Debug\LiteMQTTDemo.exe` with the
Visual Studio generator, `build` with Ninja).  The target is built when
`LITEMQTT_BUILD_EXAMPLES` is `ON`, which is the default when LiteMQTT is the
top-level project.

### Run

The demo needs a real MQTT broker listening on the given host and port; it does
not start one of its own.  Any broker will do (mosquitto, EMQX, HiveMQ, ...),
but the defaults assume one is on `127.0.0.1:1883`.

```bash
./LiteMQTTDemo
```

With a broker somewhere else:

```bash
./LiteMQTTDemo --host 192.168.1.10 --port 1883 --id my_demo
```

### Options

| Option | Default | Meaning |
|--------|---------|---------|
| `--host <addr>` | `127.0.0.1` | Broker address |
| `--port <n>` | `1883` | Broker port |
| `--id <client-id>` | `litemqtt_cpp_demo` | MQTT client id |
| `--subscribe <topic>` | `litemqtt/demo` | Topic to subscribe to (QoS 2) |
| `--publish <topic>` | `litemqtt/demo` | Topic to publish to (QoS 2) |
| `--payload <text>` | `Hello from LiteMQTT!` | Payload of every published message |
| `--username <name>` | (none) | Username, only used together with `--password` |
| `--password <pass>` | (none) | Password, only used together with `--username` |

There is no `--help`: an unknown option is ignored and the defaults stay in
effect.

### What it does

1. Installs a process-wide log handler that prints every record with its source
   location.  `demo.cc` also shows, in comments, the two alternatives: raising
   the severity threshold (`set_log_level`) and per-instance sinks
   (`set_log_handler` on the helper).
2. Registers `on_message` before `subscribe`, so the callback is installed for
   the topic's first delivery.
3. Subscribes at QoS 2 with a callback, then calls `start()`.
4. Publishes `--payload` on `--publish` every 10 ms for as long as the session
   is connected, at QoS 2, with no publish callback -- a publish that fails is
   reported only through the log.
5. On Ctrl-C, calls `stop()` (which sends DISCONNECT and joins the io thread)
   and exits.

Because `subscribe` and `publish` default to the same topic, a connected demo
prints the messages it publishes itself.  Give the two different topics to see
only what another client sends:

```bash
./LiteMQTTDemo --subscribe litemqtt/in --publish litemqtt/out
```

Note that the publish rate is fixed at one message per 10 ms: pointing the demo
at a shared broker produces a lot of traffic, so keep it on a broker of your
own.

### Reconnection

`set_reconnect_config(-1, 3000)` asks for unlimited retries 3 s apart.  Stop the
broker while the demo runs and it keeps retrying; start it again and the demo
reconnects and re-subscribes on its own.  The subscribe callback is not invoked
again for the restored subscription -- the restoration is internal to the
helper.
