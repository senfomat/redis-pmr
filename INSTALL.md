# Installing redis-pmr on Linux servers

This guide takes you from a set of plain Linux servers with Redis to a
running `redis-pmr` replication mesh. It covers Debian/Ubuntu and the RHEL
family (RHEL, Rocky, AlmaLinux, Fedora). For background and every
configuration option, see the [README](README.md).

- [Overview](#overview)
- [1. Prerequisites](#1-prerequisites)
- [2. Prepare Redis](#2-prepare-redis)
- [3. Create the service user](#3-create-the-service-user)
- [4. Build the package](#4-build-the-package)
- [5. Install uv and redis-pmr](#5-install-uv-and-redis-pmr)
- [6. Configure](#6-configure)
- [7. Smoke test](#7-smoke-test)
- [8. systemd service](#8-systemd-service)
- [9. Verify replication](#9-verify-replication)
- [10. Seed existing data](#10-seed-existing-data)
- [Upgrading redis-pmr](#upgrading-redis-pmr)
- [Uninstalling](#uninstalling)
- [Distribution notes](#distribution-notes)

## Overview

One `redis-pmr` process runs next to every Redis server. All processes use
**the same configuration file**, which lists every node of the mesh. Each
process listens to its local Redis via a Unix socket and writes changes to
the other nodes via TCP.

Layout on every host:

| Path | Content |
|---|---|
| `/opt/redis-pmr/` | Python runtime, virtual environment and executable, managed by uv |
| `/usr/local/bin/redis-pmr` | Symlink to the executable |
| `/etc/redis-pmr/config.toml` | Shared configuration (identical on all hosts) |
| `/etc/redis-pmr/env` | Per-host secrets (`PMR_*` variables) |
| `/etc/systemd/system/redis-pmr.service` | systemd unit |

Steps 2, 3 and 5–8 are repeated on every host. Step 4 runs once on a build
machine.

## 1. Prerequisites

- **Redis ≥ 7.0** on every node, ideally all on the same major.minor
  version. A node can't restore data from a newer Redis (see
  [Upgrading Redis](README.md#upgrading-redis)).
- **Synchronized clocks.** TTLs are replicated as absolute timestamps. Make
  sure `systemd-timesyncd` or `chrony` is running:

  ```bash
  timedatectl status        # "System clock synchronized: yes"
  ```

- **Network.** Every node must reach every other node on its Redis port
  (default 6379). Open it only for the peers:

  ```bash
  # Debian/Ubuntu (ufw), once per peer
  ufw allow from 10.0.0.12 to any port 6379 proto tcp

  # RHEL family (firewalld), once per peer
  firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="10.0.0.12" port port="6379" protocol="tcp" accept'
  firewall-cmd --reload
  ```

- **Python is not required** on the servers. uv installs its own
  Python ≥ 3.12 under `/opt/redis-pmr`, independent of the distribution's
  Python (Debian 12, for example, only ships 3.11).
- Outbound HTTPS from the server, once, to install uv and download Python.
  Without internet access, see the note in
  [step 5](#5-install-uv-and-redis-pmr).

## 2. Prepare Redis

### Install

```bash
# Debian/Ubuntu
apt install redis-server redis-tools

# RHEL family
dnf install redis
```

Check the version with `redis-server --version`. If your distribution ships
something older than 7.0 (e.g. the default stream on RHEL 9), use a newer
module stream (`dnf module list redis`) or the official packages from
[packages.redis.io](https://redis.io/docs/latest/operate/oss_and_stack/install/install-stack/).
Distributions that replaced Redis with Valkey are untested.

### Configure `redis.conf`

The file is `/etc/redis/redis.conf` on Debian/Ubuntu and RHEL 9+, and
`/etc/redis.conf` on older RHEL releases.

```conf
# Reachable locally and from the peers (use this host's internal address)
bind 127.0.0.1 -::1 10.0.0.11
protected-mode yes
port 6379

# Local connection for redis-pmr. Use the same path on every host so the
# configuration file stays identical.
unixsocket /run/redis/redis-server.sock
unixsocketperm 770

# Users and passwords (see below)
aclfile /etc/redis/users.acl
```

`redis-pmr` sets the required `notify-keyspace-events` flags itself on every
connect. To manage them in `redis.conf` instead, see
[Keyspace notifications](README.md#keyspace-notifications).

### Create the ACL user

Every node needs one user that `redis-pmr` uses both locally and from the
other hosts. Write it to `/etc/redis/users.acl`, adjusting the key patterns
(`~…`) and channel patterns (`&__keyspace@<db>__:…`) to your
`replication.patterns`:

```
user default on >ADMIN-PASSWORD ~* &* +@all
user pmr on >PMR-PASSWORD resetkeys ~session:* ~lock:* resetchannels &__keyspace@0__:session:* &__keyspace@0__:lock:* -@all +info +ping +psubscribe +punsubscribe +dump +pexpiretime +scan +multi +exec +restore +del +config|get +config|set +client|setname +client|setinfo
```

- Each user goes on **one line**.
- With an `aclfile`, `requirepass` is ignored. The `default` user has to be
  defined in the file, otherwise it allows access **without a password**.
- [ACL user](README.md#acl-user) in the README explains every permission.
  You can drop `+config|set` if you manage `notify-keyspace-events` in
  `redis.conf`.
- `PMR-PASSWORD` is typically the same on all nodes, so a single
  `[auth]` section covers the whole mesh.

```bash
chown redis:redis /etc/redis/users.acl
chmod 640 /etc/redis/users.acl
systemctl restart redis-server    # RHEL family: redis
```

### Test

```bash
# Locally via the socket
redis-cli -s /run/redis/redis-server.sock --user pmr --askpass ping

# From another node via TCP
redis-cli -h 10.0.0.11 --user pmr --askpass ping
```

Both must answer `PONG`.

## 3. Create the service user

`redis-pmr` runs as its own system user. Its primary group is the group that
owns the Redis socket (usually `redis`), so it can use the socket thanks to
`unixsocketperm 770`.

```bash
# Debian/Ubuntu
useradd --system --no-create-home --shell /usr/sbin/nologin --gid redis redis-pmr

# RHEL family
useradd --system --no-create-home --shell /sbin/nologin --gid redis redis-pmr
```

## 4. Build the package

On a machine with a checkout of this repository and
[uv](https://docs.astral.sh/uv/):

```bash
uv build --wheel
# → dist/redis_pmr-0.1.0-py3-none-any.whl
```

The wheel is pure Python and works on every Linux distribution and
architecture. Copy it to every host:

```bash
scp dist/redis_pmr-0.1.0-py3-none-any.whl root@node-a:/tmp/
```

## 5. Install uv and redis-pmr

Run the following as root.

Install uv system-wide (or use your distribution's `uv` package, if
available):

```bash
curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh
```

Install `redis-pmr` with every path under `/opt/redis-pmr`. Without these
variables, uv puts the Python runtime and the virtual environment under
`/root/.local`, which the service user can't read and which the systemd unit
hides (`ProtectHome=true`).

```bash
export UV_TOOL_DIR=/opt/redis-pmr/tools
export UV_TOOL_BIN_DIR=/opt/redis-pmr/bin
export UV_PYTHON_INSTALL_DIR=/opt/redis-pmr/python

uv tool install --managed-python --python 3.12 /tmp/redis_pmr-0.1.0-py3-none-any.whl
ln -sf /opt/redis-pmr/bin/redis-pmr /usr/local/bin/redis-pmr

redis-pmr --help
```

`--managed-python` makes uv use its own Python, so an upgrade of the
distribution's Python can't break the installation. You can ignore uv's
warning that `/opt/redis-pmr/bin` isn't on your `PATH`; the symlink covers
it.

> **No internet access on the server?** Run the same commands on a machine
> with the same distribution and architecture and copy `/opt/redis-pmr` and
> `/usr/local/bin/uv` over. The paths must stay identical.

## 6. Configure

### Shared configuration

Start from [config.example.toml](config.example.toml):

```bash
install -d -m 755 /etc/redis-pmr
install -m 640 -o root -g redis-pmr config.example.toml /etc/redis-pmr/config.toml
```

Adjust at least:

- `[replication] patterns`: the keys to replicate. They must match the
  key and channel patterns of the ACL user.
- `[local] url`: keep `unix:///run/redis/redis-server.sock?db=0`, and change
  `db` if you replicate another database.
- `[[nodes]]`: one entry per host. `url` is the address under which the
  **other** hosts reach this node (e.g. `redis://10.0.0.11:6379/0`). The
  local node stays in the list; it is detected automatically.
- `[auth] username = "pmr"`. Leave the password out of this file.

Then distribute **the identical file** to every host. See
[Configuration reference](README.md#configuration-reference) for all
options.

### Per-host secrets

`/etc/redis-pmr/env` holds the password. systemd reads it as root, so it can
be private to root:

```bash
install -m 600 -o root -g root /dev/null /etc/redis-pmr/env
```

```bash
# /etc/redis-pmr/env
PMR_AUTH__PASSWORD=PMR-PASSWORD
# Only if the local Redis uses different credentials:
# PMR_LOCAL__PASSWORD=...
```

No URL encoding is needed here. Every other setting can be overridden the
same way (`PMR_<SECTION>__<KEY>`).

## 7. Smoke test

Run `--check` as the service user, with the same environment the service
will get. It connects to the local Redis and to every node, changes nothing
and reports what it found:

```bash
systemd-run --pipe --wait --quiet \
  -p User=redis-pmr -p Group=redis \
  -p EnvironmentFile=/etc/redis-pmr/env \
  /usr/local/bin/redis-pmr -c /etc/redis-pmr/config.toml --check
```

```
OK   local                Redis 7.0.15, notify-keyspace-events '', missing 'Kg$lshzt' (will be set)
OK   node-a               self (detected by run_id), skipped
OK   node-b               Redis 7.0.15
OK   node-c               Redis 7.0.15
```

Every line must start with `OK`, and exactly one node must be `self`. If a
node fails because its peer isn't set up yet, finish the other hosts first
and run the check again. Common errors are listed under
[Troubleshooting](README.md#troubleshooting).

## 8. systemd service

Create `/etc/systemd/system/redis-pmr.service`:

```ini
[Unit]
Description=redis-pmr – Redis multi-master key replication
After=network-online.target redis-server.service redis.service
Wants=network-online.target

[Service]
User=redis-pmr
Group=redis
ExecStart=/usr/local/bin/redis-pmr -c /etc/redis-pmr/config.toml
EnvironmentFile=/etc/redis-pmr/env
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

`After=` names both Redis service names (`redis-server` on Debian/Ubuntu,
`redis` on the RHEL family); the one that doesn't exist is ignored.
`redis-pmr` doesn't depend on the order anyway: it waits for Redis and
reconnects.

```bash
systemctl daemon-reload
systemctl enable --now redis-pmr
systemctl status redis-pmr
journalctl -u redis-pmr -f
```

Once a minute, a stats line shows the state of every node. All peers should
be `active` and the local node `self`. See
[Logs and stats](README.md#logs-and-stats) for the fields.

## 9. Verify replication

Once the service runs on all hosts, write a key that matches your patterns
and watch it arrive. The `pmr` user may not read or write values, so use the
`default` user (`redis-cli` asks for `ADMIN-PASSWORD`):

```bash
# on node-a
redis-cli -s /run/redis/redis-server.sock --askpass SET session:install-test hello EX 300

# on node-b and node-c
redis-cli -s /run/redis/redis-server.sock --askpass GET session:install-test    # "hello"
redis-cli -s /run/redis/redis-server.sock --askpass PTTL session:install-test   # just under 300000

# on node-c: deletions are replicated, too
redis-cli -s /run/redis/redis-server.sock --askpass DEL session:install-test

# on node-a and node-b
redis-cli -s /run/redis/redis-server.sock --askpass EXISTS session:install-test # 0
```

## 10. Seed existing data

`redis-pmr` doesn't do a full sync; only keys that change after the start are
replicated. To copy keys that already exist, let **one** instance push all
matching keys once:

```bash
# on the host with the complete data
systemctl edit redis-pmr
```

```ini
[Service]
Environment=PMR_REPLICATION__RECONCILE_ON_START=true
```

```bash
systemctl restart redis-pmr
journalctl -u redis-pmr -f      # wait until pending=0 for all nodes

# remove the override again
systemctl revert redis-pmr
systemctl restart redis-pmr
```

See [Node outage](README.md#node-outage) for what happens when a node comes
back with an empty dataset.

## Upgrading redis-pmr

Build a new wheel (step 4), copy it to the hosts, then on one host after the
other:

```bash
export UV_TOOL_DIR=/opt/redis-pmr/tools
export UV_TOOL_BIN_DIR=/opt/redis-pmr/bin
export UV_PYTHON_INSTALL_DIR=/opt/redis-pmr/python

uv tool install --force --managed-python --python 3.12 /tmp/redis_pmr-<version>-py3-none-any.whl
systemctl restart redis-pmr
journalctl -u redis-pmr -n 20
```

While a host restarts, the others buffer its changes. For Redis itself, see
[Upgrading Redis](README.md#upgrading-redis).

## Uninstalling

```bash
systemctl disable --now redis-pmr
rm /etc/systemd/system/redis-pmr.service
systemctl daemon-reload

export UV_TOOL_DIR=/opt/redis-pmr/tools UV_TOOL_BIN_DIR=/opt/redis-pmr/bin
uv tool uninstall redis-pmr
rm -rf /opt/redis-pmr /etc/redis-pmr /usr/local/bin/redis-pmr
userdel redis-pmr
```

Then remove the `pmr` line from `/etc/redis/users.acl`, restart Redis, and
remove the node's `[[nodes]]` entry from the configuration on the remaining
hosts (see [Adding or removing a node](README.md#adding-or-removing-a-node)).

## Distribution notes

| | Debian / Ubuntu | RHEL family |
|---|---|---|
| Redis package | `redis-server`, `redis-tools` | `redis` |
| Redis service | `redis-server.service` | `redis.service` |
| `redis.conf` | `/etc/redis/redis.conf` | `/etc/redis/redis.conf` (9+), `/etc/redis.conf` (older) |
| Socket directory | `/run/redis`, created by the Redis unit | `/run/redis`, created via `tmpfiles.d`; check that it exists |
| Firewall | `ufw` | `firewalld` |
| `nologin` shell | `/usr/sbin/nologin` | `/sbin/nologin` |
| Mandatory access control | AppArmor: no profile for Redis by default | SELinux: a systemd service can normally use the socket in `/run/redis`. On denials, check `ausearch -m avc -ts recent`. |
