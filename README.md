# mssql-pg-migrate Airflow Pipelines

Airflow DAGs for orchestrating database migrations between MSSQL and PostgreSQL using [mssql-pg-migrate](https://github.com/johndauphine/mssql-pg-migrate).

## Features

- **DockerOperator Integration**: Run migrations in isolated containers
- **Health Checks**: Verify database connectivity before migration
- **Automatic Retry with Resume**: Failed migrations automatically retry using checkpoint resume
- **Progress Streaming**: Real-time JSON progress updates in Airflow logs
- **Exit Code Handling**: Proper task status based on migration exit codes

## Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/johndauphine/mssql-pg-migrate-airflow.git
cd mssql-pg-migrate-airflow

# Copy environment file
cp .env.example .env

# Set your Airflow UID (Linux only)
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 2. Build the Migration Image

```bash
docker compose --profile build-only build mssql-pg-migrate
```

### 3. Start Airflow

```bash
docker compose up -d

# Wait for initialization
docker compose logs -f airflow-init
```

### 4. Access Airflow UI

Open http://localhost:8080 and login with:
- Username: `airflow`
- Password: `airflow`

### 5. Configure Credentials

In the Airflow UI, go to **Admin > Variables** and set:
- `mssql_password`: Your MSSQL password
- `pg_password`: Your PostgreSQL password

### 6. Create Migration Config

```bash
cp configs/example-migration.yaml configs/my-migration.yaml
# Edit configs/my-migration.yaml with your settings
```

### 7. Trigger Migration

In the Airflow UI:
1. Go to **DAGs > mssql_pg_migration**
2. Click **Trigger DAG w/ config**
3. Set parameters:
   - `config_file`: `my-migration.yaml`
   - `dry_run`: `true` (for testing)
   - `workers`: `8`

## DAG: mssql_pg_migration

Main migration DAG with automatic retry/resume:

```
health_check → check_health → run_migration → parse_results → migration_complete
                    ↓
              health_check_failed
```

### Retry Behavior

- **First attempt**: Runs `mssql-pg-migrate run` command
- **Retry attempts**: Automatically uses `mssql-pg-migrate resume` to continue from last checkpoint
- **Retries**: 3 attempts with exponential backoff (2min → 4min → 8min)

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `config_file` | `test-migration.yaml` | Config file in `/configs` |
| `dry_run` | `false` | Preview migration without executing |
| `workers` | `4` | Number of parallel workers |

## Exit Codes

The migration tool uses standardized exit codes:

| Code | Status | Recoverable | Description |
|------|--------|-------------|-------------|
| 0 | Success | - | Migration completed |
| 1 | Failed | No | Configuration error |
| 2 | Up for Retry | Yes | Connection error |
| 3 | Failed | No | Transfer error |
| 4 | Failed | No | Validation error |
| 5 | Up for Retry | Yes | Cancelled (SIGINT/SIGTERM) |
| 6 | Failed | No | State file error |
| 7 | Up for Retry | Yes | I/O error |

## Progress Monitoring

The `--progress` flag streams JSON updates to stderr, visible in Airflow logs:

```json
{"timestamp":"2024-01-03T10:00:00Z","phase":"transfer","tables_complete":5,"tables_total":10,"rows_transferred":500000,"progress_pct":50.0,"rows_per_second":50000}
```

**Phases:**
- `extracting_schema` - Reading source table definitions
- `creating_tables` - Creating target tables
- `transfer` - Copying data
- `finalizing` - Completing transfers
- `validating` - Verifying row counts
- `completed` - Migration finished

## Directory Structure

```
mssql-pg-migrate-airflow/
├── dags/
│   └── migration_dag.py      # Airflow DAG definition
├── docker/
│   └── mssql-pg-migrate/
│       └── Dockerfile        # Migration tool image
├── configs/
│   └── example-migration.yaml
├── state/                    # Migration checkpoints (for resume)
├── logs/                     # Airflow logs (auto-created)
├── docker-compose.yaml
├── .env.example
└── README.md
```

## Environment Variables

Configure paths for different hosts via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_NETWORK` | `mssql-to-postgres-pipeline_airflow` | Docker network for database access |
| `CONFIG_MOUNT_PATH` | `/Users/john/repos/mssql-pg-migrate-airflow/configs` | Host path to config files |
| `STATE_MOUNT_PATH` | `/Users/john/repos/mssql-pg-migrate-airflow/state` | Host path for checkpoint state |

## Customization

### Using a Different mssql-pg-migrate Version

Edit `docker-compose.yaml`:

```yaml
mssql-pg-migrate:
  build:
    args:
      VERSION: "1.25.0"  # Change version here
  image: mssql-pg-migrate:1.25.0
```

Then rebuild:

```bash
docker compose --profile build-only build mssql-pg-migrate
```

### Connecting to External Databases

If your databases are outside the Docker network, update the network_mode in the DAG:

```python
DockerOperator(
    ...
    network_mode="host",  # Use host networking
)
```

### Adding Email Notifications

Update `default_args` in the DAG:

```python
default_args = {
    ...
    "email": ["your-email@example.com"],
    "email_on_failure": True,
}
```

## Troubleshooting

### "Permission denied" on Docker socket

```bash
sudo chmod 666 /var/run/docker.sock
# Or add your user to the docker group
sudo usermod -aG docker $USER
```

### Migration state not persisting

Ensure the state directory exists:

```bash
mkdir -p state
```

### Health check fails

Check database connectivity from within the Docker network:

```bash
docker run --rm --network airflow-network mssql-pg-migrate:1.24.0 \
  --config /dev/stdin health-check <<EOF
source:
  type: mssql
  host: your-mssql-host
  ...
EOF
```

## License

MIT
