# TigerData (TimescaleDB) — Local Docker Setup

> Time-series database powered by TimescaleDB, chạy local bằng Docker Compose.

---

## 1. General

| Component  |                 Image               | Port   |
|-------------|-------------------------------------|--------|
| TimescaleDB | `timescale/timescaledb:latest-pg17` | `5432` |
| pgAdmin 4   | `dpage/pgadmin4:latest`             | `5050` |

**Features:**
- TimescaleDB (PostgreSQL extension for time-series data)
- pgAdmin web UI at `http://localhost:5050`
- Data persist via Docker volumes
- Auto-restart if container crash (`unless-stopped`)
- Health check integrated

---

## 2. Structure

```
tigerdata-local/
├── docker-compose.yml
├── .env
└── init-scripts (auto run when init DB first time)
```

---

## 3. Config `.env`

Create `.env` file before running:

```env
POSTGRES_USER=admin
POSTGRES_PASSWORD=supersecret123
POSTGRES_DB=tigerdata_dev
TS_TUNE_MEMORY=2GB
TS_TUNE_NUM_CPUS=2
```

---

## 4. Start

```bash
# Run all stack (detached mode)
docker-compose up -d

# View realtime logs
docker-compose logs -f tigerdata

# View pgAdmin logs
docker-compose logs -f pgadmin
```

---

## 5. Check status

```bash
# Check containers status
docker-compose ps

# Check PostgreSQL ready to connect
docker exec tigerdata_local pg_isready -U admin -d tigerdata_dev

# Connect to psql directly
docker exec -it tigerdata_local psql -U admin -d tigerdata_dev
```

After entering psql, verify TimescaleDB extension:

```sql
SELECT default_version, installed_version
FROM pg_available_extensions
WHERE name = 'timescaledb';
```

---

## 6. pgAdmin Web UI

```
URL:      http://localhost:5050
Email:    admin@local.dev
Password: admin123
```

Add new server in pgAdmin:

```
Host:     tigerdata
Port:     5432
Database: tigerdata_dev
Username: admin
Password: supersecret123
```

---

## 7. Manage container

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (Note: lost all data)
docker-compose down -v

# Restart only DB service
docker-compose restart tigerdata

# View CPU/RAM usage
docker stats tigerdata_local

# Rebuild image (if using custom Dockerfile)
docker-compose up -d --build
```

---

## 8. Connect from application

```
postgresql://admin:supersecret123@localhost:5432/tigerdata_dev
```

Example with Python (`psycopg2`):

```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="tigerdata_dev",
    user="admin",
    password="supersecret123"
)
```

---

## 9. Notes

- Use for **local development** — not production-ready.
- Data is stored in Docker volume `tigerdata_pgdata`, **not lost when container restarts**.
- TimescaleDB automatically tunes memory/CPU based on `TS_TUNE_MEMORY` and `TS_TUNE_NUM_CPUS`.
- To disable telemetry sent to Timescale: `TIMESCALEDB_TELEMETRY=off` is already set.
