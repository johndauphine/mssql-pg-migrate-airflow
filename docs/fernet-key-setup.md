# Fernet Key Setup with Docker Secrets

This document describes how to securely configure the Airflow Fernet key using Docker secrets, ensuring the key is never exposed in environment variables or container inspection.

## Overview

Airflow uses Fernet (AES-128-CBC + HMAC-SHA256) to encrypt sensitive data at rest:
- Connection passwords
- Variable values
- Extra fields in connections

The Fernet key must be consistent across all Airflow components and persistent across restarts.

## Architecture

```
~/.secrets/fernet_key          # Key file on host (600 permissions)
        │
        ▼
Docker Secret Mount            # Mounted at /run/secrets/fernet_key
        │
        ▼
AIRFLOW__CORE__FERNET_KEY_CMD  # Executes: cat /run/secrets/fernet_key
        │
        ▼
Airflow Configuration          # Key loaded at runtime
```

**Security properties:**
- Key stored in separate file, not in `.env` or docker-compose
- Not visible via `docker inspect` or `env` command
- Only the command path visible: `AIRFLOW__CORE__FERNET_KEY_CMD=cat /run/secrets/fernet_key`

## Setup Steps

### 1. Create Secrets Directory

```bash
mkdir -p ~/.secrets
chmod 700 ~/.secrets
```

### 2. Generate Fernet Key

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode(), end='')" > ~/.secrets/fernet_key
chmod 600 ~/.secrets/fernet_key
```

Verify the key (should be exactly 44 characters, no trailing newline):
```bash
wc -c < ~/.secrets/fernet_key
# Output: 44
```

### 3. Configure docker-compose.yaml

Add the `_CMD` environment variable and secrets configuration:

```yaml
x-airflow-common: &airflow-common
  environment: &airflow-common-env
    # Load Fernet key from Docker secret via command
    AIRFLOW__CORE__FERNET_KEY_CMD: "cat /run/secrets/fernet_key"
    # ... other env vars
  secrets:
    - fernet_key
  # ... other config

# At the bottom of the file
secrets:
  fernet_key:
    file: /path/to/.secrets/fernet_key  # Use absolute path
```

### 4. Verify Configuration

After starting Airflow, verify the key is loaded correctly:

```bash
# Check env var shows only the command, not the key
docker exec airflow-scheduler env | grep FERNET
# Output: AIRFLOW__CORE__FERNET_KEY_CMD=cat /run/secrets/fernet_key

# Verify key is loaded in Airflow config
docker exec airflow-scheduler python3 -c "
from airflow.configuration import conf
key = conf.get('core', 'fernet_key')
print(f'Key loaded: {len(key)} chars, starts with: {key[:10]}...')
"
```

## Key Rotation

To rotate the Fernet key without invalidating existing encrypted data:

### 1. Generate New Key
```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode(), end='')" > ~/.secrets/fernet_key_new
```

### 2. Combine Keys (New First)
```bash
# Format: new_key,old_key
echo "$(cat ~/.secrets/fernet_key_new),$(cat ~/.secrets/fernet_key)" > ~/.secrets/fernet_key_combined
mv ~/.secrets/fernet_key_combined ~/.secrets/fernet_key
```

### 3. Restart Airflow and Rotate
```bash
docker compose restart
docker exec airflow-scheduler airflow rotate-fernet-key
```

### 4. Remove Old Key
After rotation completes, update the file to contain only the new key:
```bash
cp ~/.secrets/fernet_key_new ~/.secrets/fernet_key
docker compose restart
```

## Storing Encrypted Variables

With Fernet configured, sensitive values can be stored encrypted:

```bash
# Create encrypted variable
docker exec airflow-scheduler airflow variables set SLACK_WEBHOOK_URL "https://hooks.slack.com/..."

# Verify encryption in database
docker exec postgres psql -U airflow -d airflow -c \
  "SELECT key, substring(val, 1, 50) FROM variable WHERE key='SLACK_WEBHOOK_URL';"
# Output shows: gAAAAABl... (encrypted blob)
```

## Troubleshooting

### "Can't decrypt _val for key=X, invalid token or value"

The Fernet key doesn't match the one used to encrypt the data.

**Causes:**
- Key was regenerated without rotating existing data
- Different key across Airflow components
- Trailing newline in key file

**Fix:**
```bash
# Check for newline
wc -c < ~/.secrets/fernet_key  # Should be 44

# Remove trailing newline if present
printf '%s' "$(cat ~/.secrets/fernet_key)" > ~/.secrets/fernet_key.tmp
mv ~/.secrets/fernet_key.tmp ~/.secrets/fernet_key

# Recreate variables with correct key
docker exec airflow-scheduler airflow variables delete SLACK_WEBHOOK_URL
docker exec airflow-scheduler airflow variables set SLACK_WEBHOOK_URL "value"
```

### Key Not Loading (Random Key Used)

Airflow generates a random key if it can't read the configured one.

**Check:**
```bash
# Verify secret is mounted
docker exec airflow-scheduler cat /run/secrets/fernet_key

# Verify _CMD is set
docker exec airflow-scheduler env | grep FERNET_KEY_CMD
```

**Common issues:**
- File permissions too restrictive for container user
- `_FILE` suffix used instead of `_CMD` (not supported in Airflow 3)
- Secret not mounted in docker-compose

## File Permissions Reference

| File | Permissions | Owner |
|------|-------------|-------|
| `~/.secrets/` | 700 | user |
| `~/.secrets/fernet_key` | 600 | user |
| `/run/secrets/fernet_key` (container) | 444 | root |

## References

- [Airflow Fernet Documentation](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html)
- [Airflow Helm Chart - Fernet Key Setup](https://github.com/airflow-helm/charts/blob/main/charts/airflow/docs/faq/security/set-fernet-key.md)
