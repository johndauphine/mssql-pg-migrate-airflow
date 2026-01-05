# mssql-pg-migrate Airflow Pipelines

Airflow 3 DAGs for orchestrating database migrations between MSSQL and PostgreSQL using [mssql-pg-migrate](https://github.com/johndauphine/mssql-pg-migrate).

## Features

- **DockerOperator Integration**: Run migrations in isolated containers
- **Health Checks**: Verify database connectivity before migration
- **Automatic Retry with Resume**: Failed migrations automatically retry using checkpoint resume
- **Progress Streaming**: Real-time JSON progress updates in Airflow logs
- **Exit Code Handling**: Proper task status based on migration exit codes
- **Slack Notifications**: Success, failure, and retry notifications
- **Docker Secrets**: Secure credential management (not visible via `env`)

## Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/johndauphine/mssql-pg-migrate-airflow.git
cd mssql-pg-migrate-airflow
```

### 2. Configure Secrets

Create the secrets directory and files:

```bash
# Create secrets directory
mkdir -p ~/.secrets
chmod 700 ~/.secrets

# Generate Fernet key for Airflow encryption
openssl rand -base64 32 | tr -d '\n' > ~/.secrets/fernet_key
chmod 600 ~/.secrets/fernet_key

# Create database credential files
echo -n "YourMSSQLPassword" > ~/.secrets/mssql_password
echo -n "YourPostgresPassword" > ~/.secrets/pg_password
chmod 600 ~/.secrets/mssql_password ~/.secrets/pg_password
```

### 3. Build the Migration Image

```bash
docker compose --profile build-only build mssql-pg-migrate
```

### 4. Start Airflow

```bash
docker compose up -d

# Wait for initialization
docker compose logs -f airflow-init
```

### 5. Access Airflow UI

Open http://localhost:8080 (Airflow 3 uses simple auth - no login required in dev mode)

### 6. Configure Slack Notifications (Optional)

```bash
docker exec <scheduler-container> airflow variables set SLACK_WEBHOOK_URL "https://hooks.slack.com/services/..."
```

### 7. Create Migration Config

```bash
cp configs/test-migration.yaml configs/my-migration.yaml
# Edit configs/my-migration.yaml with your settings
```

### 8. Trigger Migration

In the Airflow UI:
1. Go to **DAGs > mssql_pg_migration**
2. Click **Trigger DAG w/ config**
3. Set parameters:
   - `config_file`: `my-migration.yaml`
   - `dry_run`: `true` (for testing)

## Secrets Architecture

Database credentials are stored as Docker secrets, not environment variables:

```
~/.secrets/
├── fernet_key       # Airflow encryption key
├── mssql_password   # MSSQL password
└── pg_password      # PostgreSQL password
```

**Security properties:**
- Passwords not visible via `docker exec <container> env`
- Not visible via `docker inspect`
- Only mounted at `/run/secrets/` inside containers

## DAG: mssql_pg_migration

Main migration DAG with automatic retry/resume:

```
health_check → check_health → run_migration → parse_results → migration_complete
                    ↓
              health_check_failed
```

### Retry Behavior

- **First attempt**: Runs `mssql-pg-migrate run` command
- **Retry attempts**: Automatically uses `mssql-pg-migrate resume` to continue from checkpoint
- **Retries**: 3 attempts with exponential backoff (2min → 4min → 8min)
- **Recoverable errors**: Connection (2), Cancelled (5), I/O (7), SIGKILL (137), SIGTERM (143)
- **Non-recoverable errors**: Config (1), Transfer (3), Validation (4), State (6) - fail immediately

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `config_file` | `test-migration.yaml` | Config file in `/configs` |
| `dry_run` | `false` | Preview migration without executing |
| `workers` | `4` | Number of parallel workers |

## Exit Codes

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
| 137 | Up for Retry | Yes | SIGKILL (container killed) |
| 143 | Up for Retry | Yes | SIGTERM (container stopped) |

## Slack Notifications

When configured, Slack notifications are sent for:
- **Success**: Tables migrated, row counts, throughput
- **Failure**: Error details, attempt count
- **Retry**: Next retry time, error reason

Set the webhook URL as an encrypted Airflow Variable:

```bash
docker exec <scheduler-container> airflow variables set SLACK_WEBHOOK_URL "https://hooks.slack.com/..."
```

## Directory Structure

```
mssql-pg-migrate-airflow/
├── dags/
│   └── migration_dag.py      # Airflow DAG definition
├── docker/
│   ├── airflow/
│   │   └── Dockerfile        # Custom Airflow image
│   └── mssql-pg-migrate/
│       └── Dockerfile        # Migration tool image
├── configs/
│   └── *.yaml                # Migration configs
├── docs/
│   └── fernet-key-setup.md   # Detailed Fernet key docs
├── state/                    # Migration checkpoints (for resume)
├── logs/                     # Airflow logs (auto-created)
└── docker-compose.yaml
```

## Environment Variables

Configure paths via environment variables (defaults use `${PWD}`):

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_NETWORK` | `mssql-to-postgres-pipeline_airflow` | Docker network for database access |
| `CONFIG_MOUNT_PATH` | `${PWD}/configs` | Host path to config files |
| `STATE_MOUNT_PATH` | `${PWD}/state` | Host path for checkpoint state |

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

If your databases are outside the Docker network, set `DATABASE_NETWORK`:

```bash
DATABASE_NETWORK=host docker compose up -d
```

## Troubleshooting

### "Permission denied" on Docker socket

```bash
sudo chmod 666 /var/run/docker.sock
# Or add your user to the docker group
sudo usermod -aG docker $USER
```

### Migration state not persisting

Ensure the state directory exists with proper permissions:

```bash
mkdir -p state
chmod 777 state
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

### Secrets not loading

Verify secrets are mounted:

```bash
docker exec <scheduler-container> ls -la /run/secrets/
docker exec <scheduler-container> cat /run/secrets/mssql_password
```

## License

MIT
