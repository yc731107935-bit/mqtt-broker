# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-10-14

### Added

- MQTT 3.1.1 protocol engine with a subset of MQTT 5.0 support
  (CONNECT/CONNACK, PUBLISH/PUBACK, SUBSCRIBE/SUBACK, UNSUBSCRIBE/UNSUBACK,
  PINGREQ/PINGRESP, DISCONNECT)
- Single-threaded, edge-triggered epoll event loop (`src/net`)
- Topic wildcard matching (`+` and `#`)
- Non-blocking TX with per-session pending buffers flushed on `EPOLLOUT`
- QoS 1 delivery with in-flight tracking and periodic retransmission (timerfd)
- Adaptive RX buffer for half-frame/coalesced-frame reassembly
- No-local-echo fan-out: publishers never receive their own messages
- Leveled stderr logger with timestamps (`src/util`)
- Unit tests for the message queue (`tests`)
- CI pipeline: build, unit tests, cppcheck, Doxygen (`.github/workflows`)

[Unreleased]: https://github.com/<org>/mqtt-broker/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/<org>/mqtt-broker/releases/tag/v1.0.0
