# mqtt-broker

A lightweight, standalone MQTT 3.1.1 broker for embedded Linux, built around a single-threaded
`epoll` event loop. It implements protocol handling and topic-based subscription forwarding only —
no business logic, no external dependencies beyond libc/pthread.

## Features

- MQTT 3.1.1 protocol with a subset of MQTT 5.0 (CONNECT/CONNACK, PUBLISH, PUBACK, SUBSCRIBE/SUBACK,
  UNSUBSCRIBE/UNSUBACK, PINGREQ/PINGRESP, DISCONNECT)
- Single-threaded, fully non-blocking `epoll` event loop (edge-triggered)
- Topic wildcard matching (`+` and `#`)
- Non-blocking TX with per-session pending buffers flushed on `EPOLLOUT`
- QoS 1 delivery with in-flight tracking and periodic retransmission (timerfd driven)
- Delivery QoS taken from the receiving subscription (downgraded QoS0 publish to QoS1 subscriber)
- Half-frame / coalesced-frame reassembly with an adaptive RX buffer (inline 2 KB, heap growth on demand)
- No-local-echo fan-out: publishers never receive their own messages
- Lightweight leveled logging to stderr with timestamps

## Project Layout

```
.
|- Makefile
|- src/
|   |- app/            process entry point
|   |   `- main.c
|   |- broker/         MQTT protocol engine
|   |   |- mqtt_broker.c/.h      sessions, fan-out, QoS1 retries, TCP listener
|   |   `- message_queue.c/.h    ring-buffer message queue for TX fan-out
|   |- net/            generic event loop
|   |   `- event_loop.c/.h       epoll-based callback event loop
|   `- util/           support utilities
|       |- log.c/.h             leveled stderr logger
|       `- time_utils.c/.h      timestamp helper
`- build/             build output (git-ignored)
```

## Building

The default Makefile targets an ARM cross toolchain:

```sh
make                  # release build -> build/release/bin/mqtt_broker
make MODE=debug       # debug build   -> build/debug/bin/mqtt_broker
make clean
```

Override the toolchain prefix if needed:

```sh
make CROSS_COMPILE=arm-linux-gnueabihf-
# or build with the host compiler:
make CROSS_COMPILE= MODE=host
```

## Running

```sh
./build/release/bin/mqtt_broker
```

The broker listens on `0.0.0.0:1883` and supports up to 8 concurrent clients.

## Testing

Unit tests live in `tests/` and run with the host compiler:

```sh
make test CROSS_COMPILE=     # builds and runs tests/test_message_queue
```

## Static Analysis & Documentation

```sh
make lint     # cppcheck (warning, style, performance, portability)
make docs     # Doxygen HTML docs -> docs/generated/html/index.html
```

## Continuous Integration

`.github/workflows/ci.yml` runs on every push/pull request:

1. Debug and release host builds (`-Wall -Wextra`)
2. Unit tests
3. cppcheck static analysis
4. Doxygen documentation build (warnings fail the build)

## Repository Conventions

- [CHANGELOG.md](CHANGELOG.md) - versioned change log (Keep a Changelog / SemVer)
- [CONTRIBUTING.md](CONTRIBUTING.md) - coding style, commit and review rules
- `.clang-format` / `.editorconfig` - uniform formatting across editors
- `Doxyfile` - API documentation configuration
- Comments and documentation are written in English (Doxygen style)

## License

Released under the [MIT License](LICENSE).
