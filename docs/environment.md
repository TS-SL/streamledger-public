# Environment

## Requirements

- Linux, or WSL2 on Windows, with Docker Engine and Docker Compose v2.
- Python 3.11+ (3.12 used throughout).
- Java 17 for the PySpark rebuild.
- Free host ports `127.0.0.1:19092` (Kafka) and `127.0.0.1:55432` (PostgreSQL). Port 55432 is used instead of 5432 so an existing local PostgreSQL does not clash.

Python packages, pinned to the versions that were tested:

| package | version | needed for |
|---|---|---|
| pytest | 8.3.3 | all tests |
| pyarrow | 21.0.0 | archive, unit tests |
| psycopg[binary] | 3.2.10 | anything touching PostgreSQL |
| confluent-kafka | 2.11.1 (librdkafka 2.11.1) | anything touching Kafka |
| pandas | 2.2.3 | PySpark rebuild |
| pyspark | 4.0.1 | PySpark rebuild |

Images: `apache/kafka:3.9.1` (KRaft, no ZooKeeper) and `postgres:17.6-alpine`. Both
publish arm64 and amd64 variants. Apache Kafka was chosen as the reference
implementation of the protocol.

## The development machine

All retained measurements ([verification-history.md](verification-history.md)) come
from one machine:

| item | value |
|---|---|
| CPU | Snapdragon X Elite X1E78100, 12 cores, ARM64 |
| host OS | Windows 11, WSL2 with Ubuntu 24.04 (aarch64, kernel 6.6.87.2-microsoft-standard-WSL2) |
| memory | 15.6 GB host, 7.7 GB visible to WSL, roughly 0.5 GB free on the host with desktop applications running |
| Docker | 29.1.3 (Docker Engine inside WSL; Unix socket only, no TCP listener) |
| Java | OpenJDK 17.0.21 |
| Python | 3.12.3 (WSL, native aarch64) |

Container limits (`compose.yaml`): Kafka 1 GiB / 2 CPUs with a 256–512 MiB heap;
PostgreSQL 512 MiB / 2 CPUs with `shared_buffers=128MB`. Spark runs as `local[2]`
with a 1 GiB driver. Because of the small amount of free memory, the documented
reconciliation flow runs Spark after the streaming stack has stopped, from a saved
database snapshot.

## Observed stack behaviour (one run each)

| measurement | value |
|---|---|
| cold `up` until both health checks pass | 8.2 s (15.1 s including topic creation and migrations) |
| `down` | 1.8 s |
| idle memory: Kafka / PostgreSQL | ~300 MiB / 16–52 MiB |
| published ports | only `127.0.0.1:19092` and `127.0.0.1:55432` (checked with `ss -ltn`) |
| topic, config and migrations survive `down`/`up` | yes |

**WSL2 idles out.** With no Windows-side `wsl.exe` session attached, the WSL VM shut
itself down (three boots within 15 minutes, no OOM kills in the journal) and took the
broker and database with it. Keep one `wsl.exe` session open while the stack runs
(for example `wsl.exe -- sleep infinity`). Containers use `restart: "no"` on purpose,
so the stack never starts by itself.

**Record timestamps.** Under `LogAppendTime` the broker overrides producer timestamps
(observed on a smoke topic). The main topic therefore uses `CreateTime`; see
[consistency-contract.md](consistency-contract.md) section 4.
