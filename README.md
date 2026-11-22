# Virtual Switch (OS Fall 2026)

Multithreaded Layer-2 virtual network switch in C11 on Linux with test client (`host`) and admin CLI (`swctl`).

## Build
- `make` : builds `switch`, `host`, and `swctl` with `-std=c11 -Wall -Wextra -Werror -g -O0 -pthread`
- `make tsan` : builds ThreadSanitizer binaries (`switch_tsan`, `host_tsan`)
- `make clean` : removes binaries, object files, and logs

## Usage
- Switch: `./switch <tcp_port> <num_ports>`
- Host: `./host <switch_ip> <tcp_port> <mac>`
- Admin CLI: `./swctl <show mac | show ports | shutdown <p> | up <p> | vlan <p> <v> | mirror <src> <dst> | clear mac | stats | stats --watch>`
- Signals: `SIGINT` (clean shutdown), `SIGHUP` (reload `switch.conf`), `SIGUSR1` (dump MAC table to `mac_dump.txt`)

## Config (`switch.conf`)
Supports `aging_time`, `mac_limit`, `rate_limit`, `port_<id>_vlan`, `port_<id>_trunk`, and `mirror_<src>`.

## Completed Features
- Tier 1 (Items 1-5): 17-byte wire protocol, per-port RX/TX threads with bounded queues, refcounted broadcast frames, striped-lock MAC table with LRU/aging/port-security, and `sem_timedwait` port cap.
- Tier 2 (Items 6-13): 5:3:1 WRR priority queues with aging, token-bucket rate limiter, VLAN isolation and trunk port, non-blocking port mirroring, live reconfiguration under strict lock ordering, `swctl` over UNIX domain socket with `PTHREAD_PROCESS_SHARED` shared memory stats, `sigwait` signal thread, and async logger thread (`switch.log`).
