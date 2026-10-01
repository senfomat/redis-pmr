# redis-pmr – Redis Poor Man's Multi-Master Replication

`redis-pmr` replicates selected keys (glob patterns) between several
independent Redis servers at **key level**, including TTLs, without any full
sync. Every node is a master: a change on any node is propagated to all the
others.

Projects that tried to bring multi-master replication to Redis (e.g. KeyDB)
are broken or abandoned. `redis-pmr` takes a deliberately simple approach
instead: it listens to keyspace notifications and copies the affected keys
with `DUMP`/`RESTORE`.

- [Features](#features)
- [How it works](#how-it-works)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Redis setup](#redis-setup)
- [Configuration reference](#configuration-reference)
- [Command line](#command-line)
- [Deployment](#deployment)
- [Operations](#operations)
- [Consistency model and limitations](#consistency-model-and-limitations)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

## Features

- **Multi-master, key level:** only keys matching your patterns are
  replicated, in every direction, with no full sync.
- **All data types:** strings, hashes, lists, sets, sorted sets, streams and
  module types are copied as opaque `DUMP` payloads.
- **TTLs included:** expiries are transferred as absolute timestamps
  (`ABSTTL`), including hash field TTLs, so a backlog never extends a key's
  lifetime.
- **One config for all hosts:** the local node is detected automatically by
  the Redis `run_id`, so nothing replicates to itself.
- **Non-blocking:** every target node has its own buffer and asyncio task. A
  slow or dead node delays nobody else.
- **Resilient:** reconnects with exponential backoff and jitter, TCP
  keepalives and health checks, and a buffered backlog per target. After the
  local Pub/Sub connection drops, keys are re-pushed via SCAN.
- **Efficient:** bursts of changes to one key are coalesced (100 × `HINCRBY`
  becomes one copy), and reads and writes are pipelined in batches.
- **Self-checking:** verifies server versions, RDB compatibility and
  `notify-keyspace-events`, and adds missing flags if you allow it.
- **Unix domain sockets** for the local connection.
- **Authentication:** `requirepass` and ACL users, with shared defaults and
  per-node overrides. Secrets can come from the environment and are never
  logged.

## How it works

One `redis-pmr` process runs next to every Redis server. All processes use the
same configuration file.

```
host A
  Redis A ──keyspace events──▶ SourceListener
     ▲                              │ dirty keys
     │                              ▼
     │                       CoalescingBuffer
     │  DUMP + PEXPIRETIME          │
     └───────────────────── SnapshotFetcher
                                    │ Mutation (fan-out)
                  ┌─────────────────┼─────────────────┐
                  ▼                 ▼                 ▼
              Target A          Target B          Target C
              (self: skipped)   buffer + task     buffer + task
                                    │                 │
                RESTORE … REPLACE ABSTTL / DEL        │
                                    ▼                 ▼
                                 Redis B           Redis C

(host B and host C run the same pipeline in the other direction)
```

1. **Listen.** The process subscribes to `__keyspace@<db>__:<pattern>` for
   every configured pattern on the local Redis.
2. **Mark dirty.** An event only marks its key as dirty. It does not say what
   happened, and it does not have to. Repeated events for the same key are
   merged while the key waits.
3. **Snapshot.** In batches, the current state of each dirty key is read
   atomically (`MULTI` / `DUMP` / `PEXPIRETIME` / `EXEC`).
4. **Fan out.** Each snapshot goes to the buffer of every target node. Newer
   snapshots replace older pending ones for the same key.
5. **Apply.** Every target task writes its batch in a pipeline:
   `RESTORE key <expire_at_ms> <payload> REPLACE ABSTTL`, or `DEL key` when the
   key no longer exists locally.

Because the *current state* is replicated rather than the individual
operation, replication is idempotent: a deletion and a modification are
handled the same way, and a SCAN-based reconcile reuses exactly the same path.

### Event handling

| Events | Action |
|---|---|
| `restore` | **ignored.** This is what the replicator itself writes on the targets; replicating it back would cause ping-pong. |
| `expired`, `evicted` | ignored. Every node expires keys itself (the absolute TTL was replicated); eviction is a local decision. |
| `new`, `overwritten`, `type_changed`, `keymiss` | ignored. Meta events that accompany a real modification. |
| everything else (`set`, `del`, `expire`, `persist`, `rename_from`/`rename_to`, `move_from`/`move_to`, `copy_to`, `hset`, `hexpired`, `lpush`, `zadd`, `xadd`, …) | key is marked dirty and its current state is copied (or deleted). |

### Self detection

The same node list is used everywhere, so every process has to find out which
entry is its own:

1. **Explicitly** with `--node NAME` or `PMR_NODE=NAME`. That node is never
   contacted.
2. **Automatically** (always active): on every connect, a target's `run_id`
   (`INFO server`) is compared with the `run_id` of the local Redis. A match
   means the entry is the local server. It doesn't matter that the local
   server is reached via a Unix socket and listed with a TCP address. The
   target is then disabled.
3. If no entry matches, a feed-only node replicates to all listed nodes.

The `run_id` changes on every Redis restart. It is compared again on every
reconnect. `INFO` is required anyway for the version checks (see
[ACL user](#acl-user)).

## Requirements

| Requirement | Why |
|---|---|
| **Redis ≥ 7.0** on all nodes | `PEXPIRETIME` (7.0), `RESTORE … ABSTTL` (5.0). Keyspace notifications themselves exist since 2.8; redis-py 8 only added the client-side helper. Subkey notifications (Redis 8.8) are not needed. |
| Targets not older than the source (major.minor) | `RESTORE` rejects `DUMP` payloads of a newer RDB format. Mixed versions are detected and reported. |
| `notify-keyspace-events` ⊇ `Kg$lshzt` | `K` keyspace channel, `g` generic commands, `$ l s h z t` data types. Set automatically by default. |
| Synchronized clocks (NTP) | TTLs are transferred as absolute unix timestamps. |
| UTF-8 key names | redis-py decodes notification channels as UTF-8. |
| Python ≥ 3.14, [uv](https://docs.astral.sh/uv/) | Runtime and packaging. |

## Installation

```bash
# as a tool, puts `redis-pmr` on the PATH
uv tool install /path/to/redis-pmr

# or from a checkout, for development
uv sync
uv run redis-pmr --help
```

## Quick start

1. Copy [config.example.toml](config.example.toml) to e.g.
   `/etc/redis-pmr/config.toml`. Adjust `patterns` and the `[[nodes]]` list,
   then deploy **the identical file** to every host.
2. Check every host:

   ```bash
   redis-pmr -c /etc/redis-pmr/config.toml --check
   ```

   ```
   OK   local                Redis 8.10.2, notify-keyspace-events '', missing 'Kg$lshzt' (will be set)
   OK   node-a               self (detected by run_id), skipped
   OK   node-b               Redis 8.10.2
   OK   node-c               Redis 8.10.2
   ```

3. Start it on every host (see [Deployment](#deployment) for systemd):

   ```bash
   redis-pmr -c /etc/redis-pmr/config.toml
   ```

## Redis setup

### Unix domain socket for the local connection (recommended)

The example configuration connects to the local Redis via a Unix domain
socket:

- no TCP overhead
- no network exposure
- the same socket path on every host keeps the config identical

In `redis.conf`:

```conf
unixsocket /run/redis/redis-server.sock
unixsocketperm 770
```

Add the user that runs `redis-pmr` to the group owning the socket (usually
`redis`). In the URL, the database is selected with the `db` query parameter:

```toml
[local]
url = "unix:///run/redis/redis-server.sock?db=0"
```

The remote nodes are reached via TCP (`redis://`) or TLS (`rediss://`).

Keep the socket path short. Unix socket paths are limited to about 104
characters on macOS and 108 on Linux.

### Keyspace notifications

With `manage_notify_config = true` (the default), `redis-pmr` adds missing
flags via `CONFIG SET` and keeps the existing ones. It does this on every
(re)connect, so the setting survives a Redis restart even without being in
`redis.conf`.

To manage it yourself, put this in `redis.conf` and set
`manage_notify_config = false`:

```conf
notify-keyspace-events "Kg$lshzt"
```

### ACL user

Every node acts as a **source** (for its local `redis-pmr`) and as a
**target** (for the other nodes' `redis-pmr`). So one user per node needs both
sets of permissions:

| Purpose | Commands |
|---|---|
| Source | `info`, `ping`, `psubscribe`, `punsubscribe`, `dump`, `pexpiretime`, `scan`, `multi`, `exec`, `config\|get`, and `config\|set` (only with `manage_notify_config = true`) |
| Target | `info`, `ping`, `restore`, `del` |
| Connection setup | `client\|setname`, `client\|setinfo` |

```
ACL SETUSER pmr on >CHANGE-ME resetkeys ~session:* ~lock:* \
  resetchannels &__keyspace@0__:session:* &__keyspace@0__:lock:* \
  -@all +info +ping +psubscribe +punsubscribe +dump +pexpiretime +scan \
  +multi +exec +restore +del +config|get +config|set \
  +client|setname +client|setinfo
```

Channel permissions for `PSUBSCRIBE` are matched **literally** against the
subscribed pattern. Grant exactly `&__keyspace@<db>__:<pattern>` per pattern,
or `allchannels`.

`CONFIG SET` is an administrative command. If you don't want to grant it,
configure `notify-keyspace-events` in `redis.conf`, set
`manage_notify_config = false` and drop `+config|set`.

This exact ACL (with `test:*` as pattern) is part of the integration tests.

### Authentication

Both `requirepass` (password only) and ACL users (username and password) are
supported. Credentials can be given in three places, from most to least
specific:

1. **In the URL** of an entry: `redis://pmr:secret@host:6379/0`, or
   `unix://pmr@/run/redis/redis-server.sock?db=0&password=secret`. If a URL
   contains any credentials, they are used as they are and never mixed with
   the ones below. Special characters must be percent-encoded.
2. **On the entry** (`[local]` or a `[[nodes]]` entry): `username` and
   `password` fields.
3. **In `[auth]`:** defaults for every server. This is the typical setup,
   since all nodes usually share one user.

Levels 2 and 3 are resolved field by field: a node with only its own
`password` still uses the `username` from `[auth]`.

To keep the shared config file free of secrets, set the password per host
through the environment, e.g. in the systemd `EnvironmentFile`:

```bash
PMR_AUTH__USERNAME=pmr
PMR_AUTH__PASSWORD=secret          # no URL encoding needed
PMR_LOCAL__PASSWORD=other-secret   # only if the local server differs
```

Credentials never leak into logs or error messages:

- Passwords are stored as `SecretStr`.
- URLs are logged with user info and `username`/`password` query values
  masked (`unix://***@/run/redis.sock?db=0&password=***`).
- Configuration validation errors don't echo input values.

## Configuration reference

The configuration is a TOML file. Every value can be overridden by an
environment variable with the prefix `PMR_` and `__` as nesting separator.
Priority, highest first: CLI option › environment › file. The `[[nodes]]`
list can only be replaced as a whole (`PMR_NODES` as JSON); use `[auth]` for
credentials shared by all nodes.

```bash
PMR_NODE=node-b
PMR_AUTH__PASSWORD=secret
PMR_LOCAL__URL="unix:///run/redis/redis-server.sock?db=1"
PMR_REPLICATION__BATCH_SIZE=512
```

Unknown keys in the file are rejected, which catches typos.

### Top level

| Key | Type | Default | Description |
|---|---|---|---|
| `node` | string | – | Name of the local node. Usually omitted (detected by `run_id`). Must exist in `[[nodes]]`. |

### `[auth]`

Default credentials for all servers (see [Authentication](#authentication)).

| Key | Type | Default | Description |
|---|---|---|---|
| `username` | string | – | ACL user. Omit for `requirepass` (the `default` user). |
| `password` | string (secret) | – | Password. Best set via `PMR_AUTH__PASSWORD`. |

### `[local]`

| Key | Type | Default | Description |
|---|---|---|---|
| `url` | string | `redis://localhost:6379/0` | Local Redis server (`redis://`, `rediss://`, `unix://`). The database in the URL is the watched database. |
| `username` | string | from `[auth]` | ACL user of the local server. |
| `password` | string (secret) | from `[auth]` | Password of the local server. |
| `manage_notify_config` | bool | `true` | Add missing `notify-keyspace-events` flags via `CONFIG SET`. If `false`, only check and refuse to start when flags are missing. |

### `[replication]`

| Key | Type | Default | Description |
|---|---|---|---|
| `patterns` | list of strings | **required** | Glob patterns (Redis `PSUBSCRIBE`/`SCAN MATCH` syntax) of keys to replicate. |
| `batch_size` | int | `256` | Keys per `DUMP` and per `RESTORE` pipeline. |
| `max_pending_per_target` | int | `100000` | Distinct keys buffered per target. When it is full, new keys are dropped and a reconcile for that target is scheduled once it is connected again. |
| `reconcile_on_start` | bool | `false` | Push all keys matching the patterns once after the first connect. |
| `reconcile_on_reconnect` | bool | `true` | Push all keys matching the patterns after the local Pub/Sub connection was re-established, because events during the gap are lost. |
| `scan_count` | int | `1000` | `COUNT` hint for `SCAN` during reconciles. |
| `shutdown_drain_timeout` | float | `5.0` | Seconds to wait for pending mutations on SIGTERM/SIGINT. |
| `stats_interval` | float | `60.0` | Seconds between stats log lines; `0` disables them. |
| `min_server_version` | string | `"7.0"` | Minimum Redis version accepted on all nodes. |

### `[backoff]`

| Key | Type | Default | Description |
|---|---|---|---|
| `initial` | float | `0.5` | First reconnect delay in seconds. |
| `maximum` | float | `30.0` | Upper bound of the delay; also the retry interval for incompatible servers and failed reconciles. |
| `factor` | float | `2.0` | Growth factor per failed attempt. A random jitter of 50–100 % is applied. |

### `[connection]`

Applies to every connection.

| Key | Type | Default | Description |
|---|---|---|---|
| `socket_timeout` | float | `10.0` | Read/write timeout in seconds. |
| `connect_timeout` | float | `5.0` | Connect timeout in seconds. |
| `health_check_interval` | float | `10.0` | Idle connections are pinged after this many seconds. |
| `keepalive_idle` | int | `10` | TCP keepalive: idle seconds before the first probe (TCP only). |
| `keepalive_interval` | int | `5` | TCP keepalive: seconds between probes (TCP only). |
| `keepalive_count` | int | `3` | TCP keepalive: failed probes until the connection is dropped (TCP only). |

### `[[nodes]]`

At least one entry. List **all** nodes of the mesh, including the local one.

| Key | Type | Description |
|---|---|---|
| `name` | string | Unique name (`A-Z a-z 0-9 _ . -`), used in logs and for `--node`. |
| `url` | string | Address under which the *other* hosts reach this node. |
| `username` | string | ACL user, if it differs from `[auth]`. |
| `password` | string (secret) | Password, if it differs from `[auth]`. |

## Command line

```
Usage: redis-pmr [OPTIONS]

  Replicate selected Redis keys between multiple masters.

Options:
  -c, --config FILE               TOML configuration file (identical on all
                                  nodes).  [required]
  --node TEXT                     Name of the node this process runs on
                                  (default: detected by run_id).
  --log-level [debug|info|warning|error]
                                  [default: info]
  --check                         Only check all servers, then exit.
  -h, --help                      Show this message and exit.
```

- `--check` connects to the local server and every node, and changes nothing.
  It reports versions, missing notify flags, RDB compatibility and which node
  is *self*. The exit code is `0` if everything is fine and `1` otherwise.
  Useful as a deployment smoke test.
- An invalid configuration exits immediately with a validation message. So
  does a failed preflight of the local server on the very first connect (e.g.
  too old, or flags missing with `manage_notify_config = false`). Later
  failures are retried.

## Deployment

Run one instance per Redis host, for example with systemd
(`/etc/systemd/system/redis-pmr.service`):

```ini
[Unit]
Description=redis-pmr – Redis multi-master key replication
After=network-online.target redis-server.service
Wants=network-online.target

[Service]
User=redis-pmr
Group=redis
ExecStart=/usr/local/bin/redis-pmr -c /etc/redis-pmr/config.toml
# PMR_AUTH__USERNAME, PMR_AUTH__PASSWORD, optionally PMR_NODE, … (chmod 600)
EnvironmentFile=-/etc/redis-pmr/env
Restart=always
RestartSec=5
# Longer than replication.shutdown_drain_timeout
TimeoutStopSec=15
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now redis-pmr
journalctl -u redis-pmr -f
```

The process doesn't need to start after Redis: it waits and reconnects. On
`SIGTERM`/`SIGINT`, it stops listening and spends up to
`shutdown_drain_timeout` seconds applying pending mutations before it exits.

## Operations

### Logs and stats

Every `stats_interval` seconds, one line is logged:

```
stats: events=1532 reconciled=0 dirty=0 snapshots=1204 node-a=self/pending:0/applied:0/errors:0/dropped:0 node-b=active/pending:0/applied:1204/errors:0/dropped:0
```

| Field | Meaning |
|---|---|
| `events` | Keyspace events that marked a key dirty. |
| `reconciled` | Keys queued by SCAN reconciles. |
| `dirty` | Keys waiting to be snapshotted. |
| `snapshots` | Key states read from the local server. |
| `<node>=<state>` | `connecting`, `active`, `incompatible` (version/RDB check failed, retried every `backoff.maximum` seconds) or `self`. |
| `pending` | Mutations buffered for this node. Grows while the node is unreachable. |
| `applied` / `errors` | Successful `RESTORE`/`DEL` commands and per-key failures. |
| `dropped` | Mutations lost to a full buffer (followed by a reconcile). |

Use `--log-level debug` to also see repeated connection failures. By default
only the first failure of an outage is logged.

### Node outage

The other nodes keep buffering mutations for the missing node, at most one
per key and at most `max_pending_per_target` keys. When the node is back,
the backlog is applied. If the buffer overflowed, the patterns are
reconciled for that node. **Deletions** dropped from an overflowing buffer
are not recovered by the reconcile.

Restarting a node with an empty dataset doesn't refill it from the others.
There is no full sync; only keys that change afterwards arrive. To seed a
node, run any one instance with `reconcile_on_start = true` once, or copy the
data beforehand.

### Adding or removing a node

1. Add or remove the `[[nodes]]` entry in the config and distribute it to all
   hosts.
2. Restart `redis-pmr` everywhere. Configuration is read only at startup.
3. Seed a new node as described above if it needs existing keys.

### Upgrading Redis

`DUMP` payloads of a newer Redis can't be restored on an older one. During a
rolling upgrade, already-upgraded nodes report the older ones as
`incompatible` and buffer their mutations until those are upgraded too.
Upgrade all nodes in quick succession and keep `max_pending_per_target`
large enough for the window.

## Consistency model and limitations

`redis-pmr` provides **eventual consistency per key**, with *last applied
wins*. It is not a consensus system.

- **No conflict resolution.** Concurrent writes to the same key on different
  nodes can leave the nodes with different values. There are no vector clocks
  and no timestamps. Design your key space so that a key is usually written by
  one node at a time (e.g. sessions pinned to a node, locks with unique
  owners).
- **Echo-delete race.** A deletes k → B deletes k → B reports `del` → B sends
  `DEL k` back to A. If A recreated k within that round trip (milliseconds),
  the new value is deleted again.
- **No multi-key atomicity.** `MULTI`/`EXEC` blocks and Lua scripts are
  replicated key by key.
- **`RESTORE` issued by applications is not replicated.** It is
  indistinguishable from the replicator's own writes.
- **Pub/Sub is fire-and-forget.** Events during a disconnect of the local
  listener are lost. The reconcile after the reconnect re-pushes existing
  keys, but deletions in that gap stay unreplicated.
- **No full sync**, by design.
- **Standalone Redis only.** No Redis Cluster; one process watches exactly
  one database.
- **UTF-8 key names only.**
- **Large keys** are copied as a whole on every change. A 50 MB hash that
  gets one `HSET` per second means 50 MB of traffic per second and target.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `notify-keyspace-events is '…', missing flags '…' (manage_notify_config is disabled)` | Add the flags in `redis.conf`, or allow `redis-pmr` to set them. |
| `cannot set notify-keyspace-events …: NOPERM` / `unknown command` | `CONFIG` is denied (ACL) or renamed (managed Redis). Set the flags server-side and use `manage_notify_config = false`. |
| `Redis X is too old, at least 7.0 is required` | Upgrade Redis. Older versions lack `PEXPIRETIME`. |
| Node shown as `incompatible`: `cannot RESTORE payloads of the local Redis …` | The target runs an older major.minor than this node. Upgrade it. |
| `Error 2 connecting to … No such file or directory` (unix socket) | Wrong socket path or `unixsocket` not enabled. Check `redis-cli -s <path> ping`. |
| `Error 13 … Permission denied` (unix socket) | The `redis-pmr` user lacks access. Check `unixsocketperm` and group membership. |
| `WRONGPASS invalid username-password pair` / `NOAUTH Authentication required`, node stays `connecting` | Wrong or missing credentials for that server. Check `[auth]`, the entry's own fields and `PMR_*` variables. Credentials in the URL override both. Special characters in URL passwords must be percent-encoded. |
| `NOPERM … 'info'` and no connection becomes `active` | `INFO` is required for version checks and self detection. Allow `+info`. |
| `Error setting client name` | The ACL lacks `+client\|setname`. |
| Keys don't arrive, `events=0` | The pattern doesn't match, the database differs (the `db` in `[local].url`), or the flags were reset. Run `--check`. |
| `pending` keeps growing | The target is unreachable or too slow. Check the network and the target's logs; consider a larger `batch_size`. |
| Values differ between nodes | Concurrent writes (see the consistency model). The next change to the key re-aligns all nodes. |

## Development

```bash
uv sync
uv run pytest                          # unit and integration tests
uvx ruff format . && uvx ruff check .  # formatting and linting
```

The integration tests start throw-away `redis-server` processes on free
ports and are skipped if `redis-server` isn't installed. They cover all data
types, TTLs, deletes and renames, self detection, ping-pong freedom, target
outages, local Redis restarts and Pub/Sub reconnects. Authentication is
covered too: `requirepass`, the README ACL user, credentials in URLs, and
wrong passwords, which must stay isolated and never appear in logs.

### Project layout

| Module | Responsibility |
|---|---|
| [`cli.py`](src/redis_pmr/cli.py) | click entry point, `--check`, signal handling |
| [`config.py`](src/redis_pmr/config.py) | pydantic-settings models, TOML and environment loading |
| [`app.py`](src/redis_pmr/app.py) | `Replicator` wiring, graceful shutdown, stats, `run_check` |
| [`source.py`](src/redis_pmr/source.py) | Keyspace notification listener, reconnects, SCAN reconciles |
| [`fetcher.py`](src/redis_pmr/fetcher.py) | Batched atomic `DUMP` + `PEXPIRETIME` snapshots, fan-out |
| [`target.py`](src/redis_pmr/target.py) | Per-node buffer, connection, self/version checks, `RESTORE`/`DEL` |
| [`buffer.py`](src/redis_pmr/buffer.py) | Bounded coalescing buffer |
| [`preflight.py`](src/redis_pmr/preflight.py) | Version, RDB and notify-flag checks |
| [`identity.py`](src/redis_pmr/identity.py) | Local server identity (`run_id`) |
| [`events.py`](src/redis_pmr/events.py) | Event classification |
| [`clients.py`](src/redis_pmr/clients.py) | Redis client factory (timeouts, keepalive, retries) |
| [`backoff.py`](src/redis_pmr/backoff.py) | Exponential backoff with jitter |
