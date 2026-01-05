"""
Database Migration DAG using mssql-pg-migrate

This DAG orchestrates database migrations between MSSQL and PostgreSQL
using the mssql-pg-migrate tool via DockerOperator.

Features:
- Health check before migration
- Automatic retry with resume on failure
- Streaming progress via --progress flag
- Proper exit code handling for recoverable vs non-recoverable errors

Airflow 3 compatible.
"""

from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
# Airflow 3: standard operators moved to providers-standard package
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.providers.docker.exceptions import DockerContainerFailedException
from docker.types import Mount
from airflow.models import Variable
import json
import os
import re
import requests


# mssql-pg-migrate exit codes
EXIT_CODES = {
    0: "Success",
    1: "Configuration error (non-recoverable)",
    2: "Connection error (recoverable)",
    3: "Transfer error (non-recoverable)",
    4: "Validation error (non-recoverable)",
    5: "Cancelled (recoverable)",
    6: "State error (non-recoverable)",
    7: "I/O error (recoverable)",
    # Docker/system signals (128 + signal number)
    137: "SIGKILL - container killed (recoverable)",
    143: "SIGTERM - container stopped (recoverable)",
}

RECOVERABLE_CODES = {2, 5, 7, 137, 143}

# Network where databases are running
DATABASE_NETWORK = os.getenv("DATABASE_NETWORK", "mssql-to-postgres-pipeline_airflow")

# Config mount path - must be set via environment variable (host path for DockerOperator)
CONFIG_MOUNT_PATH = os.getenv("CONFIG_MOUNT_PATH")
if not CONFIG_MOUNT_PATH:
    raise ValueError("CONFIG_MOUNT_PATH environment variable must be set")

# State directory for checkpoints (must be shared between retries)
STATE_MOUNT_PATH = os.getenv("STATE_MOUNT_PATH")
if not STATE_MOUNT_PATH:
    raise ValueError("STATE_MOUNT_PATH environment variable must be set")

# Secrets directory for file-based secrets (optional - for ${file:/run/secrets/...} syntax)
SECRETS_MOUNT_PATH = os.getenv("SECRETS_MOUNT_PATH", "")

# Docker image for mssql-pg-migrate (default or test override)
MIGRATE_IMAGE = os.getenv("MIGRATE_IMAGE", "mssql-pg-migrate:1.24.0")

# Database credentials - read from Docker secrets (not env vars for security)
def get_secret(name: str, fallback_env: str = None) -> str:
    """Read secret from Docker secret mount, fallback to env var."""
    try:
        with open(f"/run/secrets/{name}") as f:
            return f.read().strip()
    except FileNotFoundError:
        if fallback_env:
            return os.getenv(fallback_env, "")
        return ""


def get_db_env_vars() -> dict:
    """Get database credentials from secrets."""
    return {
        "MSSQL_PASSWORD": get_secret("mssql_password", "MSSQL_PASSWORD"),
        "PG_PASSWORD": get_secret("pg_password", "PG_PASSWORD"),
    }

# Slack webhook URL for notifications (encrypted in Airflow Variable)
def get_slack_webhook_url():
    """Get Slack webhook URL from Airflow Variable."""
    try:
        return Variable.get("SLACK_WEBHOOK_URL", default_var="")
    except Exception:
        return ""


def send_slack_notification(status: str, config_file: str, dag_id: str = "mssql_pg_migration",
                           run_id: str = None, start_time: str = None, details: dict = None, error: str = None):
    """Send Slack notification with migration details."""
    webhook_url = get_slack_webhook_url()
    if not webhook_url:
        return

    # Status emoji and titles
    status_config = {
        "SUCCESS": {"emoji": ":white_check_mark:", "title": "DAG Success"},
        "FAILED": {"emoji": ":x:", "title": "DAG Failed"},
        "RETRYING": {"emoji": ":warning:", "title": "DAG Retrying"},
    }
    cfg = status_config.get(status, {"emoji": ":white_circle:", "title": f"DAG {status}"})

    # Build summary text
    if status == "SUCCESS" and details:
        tables = details.get("tables", 0)
        rows = details.get("rows", 0)
        speed = details.get("speed")
        if speed:
            summary = f"Migration pipeline completed successfully. Migrated {tables} tables with {rows:,} total rows. Throughput: {speed:,} rows/sec."
        else:
            summary = f"Migration pipeline completed successfully. Migrated {tables} tables with {rows:,} total rows."
    elif status == "FAILED":
        summary = f"Migration pipeline failed. {error or 'Unknown error'}"
    elif status == "RETRYING":
        attempt = details.get("attempt", "1/4") if details else "1/4"
        summary = f"Migration pipeline retrying. Attempt {attempt}. Reason: {error or 'Unknown'}"
    else:
        summary = f"Migration status: {status}"

    # Build message blocks
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{cfg['emoji']} {cfg['title']}: {dag_id}", "emoji": True}
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": summary}
        }
    ]

    # Build fields grid
    fields = [
        {"type": "mrkdwn", "text": f"*DAG*\n{dag_id}"},
        {"type": "mrkdwn", "text": f"*Run ID*\n{run_id or 'N/A'}"},
    ]

    if start_time:
        fields.append({"type": "mrkdwn", "text": f"*Started*\n{start_time}"})
    if details and "duration" in details:
        fields.append({"type": "mrkdwn", "text": f"*Duration*\n{details['duration']}"})

    if details:
        if "tables" in details:
            fields.append({"type": "mrkdwn", "text": f"*Tables*\n{details['tables']}"})
        if "rows" in details:
            fields.append({"type": "mrkdwn", "text": f"*Total Rows*\n{details['rows']:,}"})
        if "speed" in details:
            fields.append({"type": "mrkdwn", "text": f"*Throughput*\n{details['speed']:,} rows/sec"})
        if "attempt" in details:
            fields.append({"type": "mrkdwn", "text": f"*Attempt*\n{details['attempt']}"})
        if "next_retry" in details:
            fields.append({"type": "mrkdwn", "text": f"*Next Retry*\n{details['next_retry']}"})

    # Add fields in groups of 2 (Slack limit per section)
    for i in range(0, len(fields), 2):
        blocks.append({"type": "section", "fields": fields[i:i+2]})

    # Add table list for success notifications
    if status == "SUCCESS" and details and "table_list" in details:
        table_lines = [f"• {name}: {rows:,} rows" for name, rows in details["table_list"]]
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Tables Migrated:*\n" + "\n".join(table_lines)}
        })

    # Add error block if present and not already in summary
    if error and status == "FAILED":
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Error Details:*\n```{error}```"}
        })

    payload = {"blocks": blocks}

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to send Slack notification: {e}")


def parse_migration_output(output: str) -> dict:
    """Parse migration output to extract details."""
    details = {}
    if not output:
        return details

    table_list = []  # List of (table_name, row_count) tuples

    for line in output.split("\n"):
        # Extract event content if line is JSON log format
        event_content = line
        if line.startswith("{"):
            try:
                log_entry = json.loads(line)
                event_content = log_entry.get("event", "")
                # Check JSON progress for completed status
                if event_content.startswith("{"):
                    try:
                        progress = json.loads(event_content)
                        if progress.get("phase") == "completed":
                            details["rows"] = progress.get("rows_transferred", 0)
                            details["speed"] = progress.get("rows_per_second", 0)
                            # Don't return yet - continue to get table names
                    except json.JSONDecodeError:
                        pass
            except json.JSONDecodeError:
                pass

        # "Migration complete: 2 tables, 1401431 rows in 2s (785105 rows/sec)"
        match = re.search(r"Migration complete: (\d+) tables?, ([\d,]+) rows? in ([^\(]+) \((\d+) rows/sec\)", event_content)
        if match:
            details["tables"] = int(match.group(1))  # Capture table count from summary
            details["rows"] = int(match.group(2).replace(",", ""))
            details["duration"] = match.group(3).strip()
            details["speed"] = int(match.group(4))
            # Don't return yet - continue to get table names if available

        # Parse individual table completion lines
        # "[INFO] Badges                         OK 1102023 rows"
        table_match = re.search(r"\[INFO\]\s+(\w+)\s+OK\s+([\d,]+)\s+rows", event_content)
        if table_match:
            table_name = table_match.group(1)
            row_count = int(table_match.group(2).replace(",", ""))
            table_list.append((table_name, row_count))

    # Set table count and list
    if table_list:
        details["tables"] = len(table_list)
        details["table_list"] = table_list

    return details


def on_migration_success(context):
    """Callback for successful migration."""
    ti = context["ti"]
    dag_run = context["dag_run"]
    config_file = context["params"].get("config_file", "unknown")
    output = ti.xcom_pull(task_ids="run_migration")
    details = parse_migration_output(output)

    # Try to get table list from log file (XCom only has last line)
    if "table_list" not in details:
        try:
            log_dir = f"/opt/airflow/logs/dag_id={dag_run.dag_id}/run_id={dag_run.run_id}/task_id={ti.task_id}"
            log_file = f"{log_dir}/attempt={ti.try_number}.log"
            with open(log_file, "r") as f:
                log_content = f.read()
                log_details = parse_migration_output(log_content)
                if "table_list" in log_details:
                    details["table_list"] = log_details["table_list"]
        except Exception:
            pass  # Log file not accessible, continue without table list

    # Get timing info
    start_time = ti.start_date.strftime("%Y-%m-%d %H:%M:%S UTC") if ti.start_date else None
    run_id = dag_run.run_id if dag_run else None

    send_slack_notification(
        "SUCCESS", config_file,
        dag_id="mssql_pg_migration",
        run_id=run_id,
        start_time=start_time,
        details=details
    )


def extract_error_message(exception) -> str:
    """Extract meaningful error message from exception chain."""
    if not exception:
        return "Task failed"

    error_msg = str(exception)

    # Check the exception chain for StatusCode (Docker exit code)
    exc = exception
    while exc:
        exc_str = str(exc)
        match = re.search(r"StatusCode['\"]?:\s*(\d+)", exc_str)
        if match:
            exit_code = int(match.group(1))
            exit_desc = EXIT_CODES.get(exit_code, f"Exit code {exit_code}")
            return f"{exit_desc} (exit code {exit_code})"
        exc = getattr(exc, "__cause__", None)

    # If no StatusCode found, use the main exception message
    # Check if it's already a descriptive error from AirflowFailException
    if "non-recoverable error:" in error_msg:
        # Extract the error description after the colon
        parts = error_msg.split("non-recoverable error:")
        if len(parts) > 1:
            return parts[1].strip()

    return error_msg or "Task failed"


def get_exception_from_context(context) -> str:
    """Get exception message from Airflow context, handling Airflow 3 differences."""
    # Try to get error from XCom (pushed by MigrationDockerOperator)
    ti = context.get("ti")
    if ti:
        try:
            xcom_error = ti.xcom_pull(key="migration_error", task_ids="run_migration")
            if xcom_error:
                return xcom_error
        except Exception:
            pass

    # Try standard exception key
    exception = context.get("exception")
    if exception:
        return extract_error_message(exception)

    # Try reason key (used in some callbacks)
    reason = context.get("reason")
    if reason:
        return str(reason)

    return "Task failed (no exception details available)"


def on_migration_failure(context):
    """Callback for failed migration."""
    ti = context["ti"]
    dag_run = context["dag_run"]
    config_file = context["params"].get("config_file", "unknown")

    error_msg = get_exception_from_context(context)

    # Get timing info
    start_time = ti.start_date.strftime("%Y-%m-%d %H:%M:%S UTC") if ti.start_date else None
    run_id = dag_run.run_id if dag_run else None

    details = {
        "attempt": f"{ti.try_number}/{ti.max_tries + 1}"
    }
    send_slack_notification(
        "FAILED", config_file,
        dag_id="mssql_pg_migration",
        run_id=run_id,
        start_time=start_time,
        details=details,
        error=error_msg
    )


def on_health_check_failure(context):
    """Callback for failed health check."""
    ti = context["ti"]
    dag_run = context["dag_run"]
    config_file = context["params"].get("config_file", "unknown")

    # Get timing info
    start_time = ti.start_date.strftime("%Y-%m-%d %H:%M:%S UTC") if ti.start_date else None
    run_id = dag_run.run_id if dag_run else None

    details = {
        "attempt": f"{ti.try_number}/{ti.max_tries + 1}"
    }
    send_slack_notification(
        "FAILED", config_file,
        dag_id="mssql_pg_migration",
        run_id=run_id,
        start_time=start_time,
        details=details,
        error="Health check failed - database connectivity issue"
    )


def on_health_check_retry(context):
    """Callback for health check retry."""
    ti = context["ti"]
    dag_run = context["dag_run"]
    config_file = context["params"].get("config_file", "unknown")

    # Get timing info
    start_time = ti.start_date.strftime("%Y-%m-%d %H:%M:%S UTC") if ti.start_date else None
    run_id = dag_run.run_id if dag_run else None

    # Get retry delay
    retry_delay = context.get("retry_delay", timedelta(minutes=2))

    details = {
        "attempt": f"{ti.try_number}/{ti.max_tries + 1}",
        "next_retry": str(retry_delay),
    }
    send_slack_notification(
        "RETRYING", config_file,
        dag_id="mssql_pg_migration",
        run_id=run_id,
        start_time=start_time,
        details=details,
        error="Health check failed - retrying database connectivity"
    )


def on_task_failure(context):
    """Generic callback for task failures."""
    ti = context["ti"]
    dag_run = context["dag_run"]
    config_file = context["params"].get("config_file", "unknown")
    task_id = ti.task_id

    start_time = ti.start_date.strftime("%Y-%m-%d %H:%M:%S UTC") if ti.start_date else None
    run_id = dag_run.run_id if dag_run else None

    send_slack_notification(
        "FAILED", config_file,
        dag_id="mssql_pg_migration",
        run_id=run_id,
        start_time=start_time,
        error=f"Task '{task_id}' failed"
    )


def on_migration_retry(context):
    """Callback for migration retry."""
    ti = context["ti"]
    dag_run = context["dag_run"]
    config_file = context["params"].get("config_file", "unknown")

    error_msg = get_exception_from_context(context)

    # Get timing info
    start_time = ti.start_date.strftime("%Y-%m-%d %H:%M:%S UTC") if ti.start_date else None
    run_id = dag_run.run_id if dag_run else None

    # Get retry delay
    retry_delay = context.get("retry_delay", timedelta(minutes=2))

    details = {
        "attempt": f"{ti.try_number}/{ti.max_tries + 1}",
        "next_retry": str(retry_delay),
    }
    send_slack_notification(
        "RETRYING", config_file,
        dag_id="mssql_pg_migration",
        run_id=run_id,
        start_time=start_time,
        details=details,
        error=error_msg
    )


# Default arguments for all tasks
default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,  # Retry up to 3 times on failure
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=4),
}


def check_health_result(**context):
    """Branch based on health check result."""
    ti = context["ti"]
    health_output = ti.xcom_pull(task_ids="health_check")

    if health_output and "HEALTHY" in health_output:
        return "run_migration"
    return "health_check_failed"


def parse_migration_result(**context):
    """Parse and log migration results."""
    ti = context["ti"]
    output = ti.xcom_pull(task_ids="run_migration")

    if output:
        print(f"Migration output:\n{output}")
        # Log any JSON progress lines
        for line in output.split("\n"):
            if line.startswith("{"):
                try:
                    progress = json.loads(line)
                    print(f"Progress: {progress}")
                except json.JSONDecodeError:
                    pass


class MigrationDockerOperator(DockerOperator):
    """
    Custom DockerOperator that uses 'resume' command on retry attempts.

    On first attempt (try_number=1): runs 'run' command
    On retry attempts (try_number>1): runs 'resume' command to continue from checkpoint

    Only retries on recoverable exit codes (2=connection, 5=cancelled, 7=I/O).
    Non-recoverable errors (1=config, 3=transfer, 4=validation, 6=state) fail immediately.
    """

    def execute(self, context):
        ti = context["ti"]
        try_number = ti.try_number

        # Modify command based on retry attempt
        if try_number > 1:
            # On retry, use resume command instead of run
            self.log.info(f"Retry attempt {try_number} - using 'resume' command")
            # Replace 'run' with 'resume' in the command
            self.command = [
                c if c != "run" else "resume"
                for c in self.command
            ]
            # Remove dry-run flag on resume (doesn't make sense)
            self.command = [c for c in self.command if "--dry-run" not in c]
        else:
            self.log.info("First attempt - using 'run' command")

        try:
            return super().execute(context)
        except DockerContainerFailedException as e:
            # Extract exit code from exception
            exit_code = None
            if hasattr(e, 'result') and isinstance(e.result, dict):
                exit_code = e.result.get('StatusCode')
            elif 'StatusCode' in str(e):
                # Parse from error message: "Docker container failed: {'StatusCode': 4}"
                match = re.search(r"'StatusCode':\s*(\d+)", str(e))
                if match:
                    exit_code = int(match.group(1))

            if exit_code is not None:
                exit_desc = EXIT_CODES.get(exit_code, f"Unknown exit code {exit_code}")
                error_msg = f"{exit_desc} (exit code {exit_code})"

                # Push error to XCom for callback to retrieve
                ti.xcom_push(key="migration_error", value=error_msg)

                if exit_code in RECOVERABLE_CODES:
                    self.log.warning(
                        f"Migration failed with recoverable exit code {exit_code}: {exit_desc}. "
                        "Task will be retried with 'resume' command."
                    )
                    raise  # Re-raise for retry
                else:
                    self.log.error(
                        f"Migration failed with non-recoverable exit code {exit_code}: {exit_desc}. "
                        "Task will NOT be retried."
                    )
                    # Raise AirflowFailException to fail without retry
                    raise AirflowFailException(
                        f"Migration failed with non-recoverable error: {exit_desc}"
                    ) from e

            # Push generic error to XCom
            ti.xcom_push(key="migration_error", value=f"Docker container failed: {str(e)}")
            # If we couldn't determine exit code, re-raise for default behavior
            raise


with DAG(
    dag_id="mssql_pg_migration",
    default_args=default_args,
    description="Migrate data from MSSQL to PostgreSQL with automatic retry/resume",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["migration", "mssql", "postgres"],
    params={
        "config_file": "test-migration.yaml",
        "dry_run": False,
        "workers": 4,
    },
    doc_md=__doc__,
) as dag:

    # Health check task - verify database connectivity
    # Build health check mounts list
    health_check_mounts = [
        Mount(
            source=CONFIG_MOUNT_PATH,
            target="/config",
            type="bind",
            read_only=True,
        ),
        Mount(
            source=STATE_MOUNT_PATH,
            target="/state",
            type="bind",
        ),
    ]
    if SECRETS_MOUNT_PATH:
        health_check_mounts.append(Mount(
            source=SECRETS_MOUNT_PATH,
            target="/run/secrets",
            type="bind",
            read_only=True,
        ))

    health_check = DockerOperator(
        task_id="health_check",
        image=MIGRATE_IMAGE,
        command=[
            "--config", "/config/{{ params.config_file }}",
            "health-check",
        ],
        mounts=health_check_mounts,
        environment=get_db_env_vars(),
        docker_url="unix://var/run/docker.sock",
        network_mode=DATABASE_NETWORK,
        auto_remove="success",
        do_xcom_push=True,
        mount_tmp_dir=False,
        retries=2,  # Health check can retry on transient network issues
        on_failure_callback=on_health_check_failure,
        on_retry_callback=on_health_check_retry,
    )

    # Branch based on health check
    check_health = BranchPythonOperator(
        task_id="check_health",
        python_callable=check_health_result,
        on_failure_callback=on_task_failure,
    )

    # Health check failed - stop pipeline
    health_failed = EmptyOperator(
        task_id="health_check_failed",
    )

    # Run the actual migration (with automatic resume on retry)
    # Build mounts list - always include config and state
    mounts = [
        Mount(
            source=CONFIG_MOUNT_PATH,
            target="/config",
            type="bind",
            read_only=True,
        ),
        Mount(
            source=STATE_MOUNT_PATH,
            target="/state",
            type="bind",
        ),
    ]
    # Optionally mount secrets directory for ${file:/run/secrets/...} syntax
    if SECRETS_MOUNT_PATH:
        mounts.append(Mount(
            source=SECRETS_MOUNT_PATH,
            target="/run/secrets",
            type="bind",
            read_only=True,
        ))

    run_migration = MigrationDockerOperator(
        task_id="run_migration",
        image=MIGRATE_IMAGE,
        command=[
            "--config", "/config/{{ params.config_file }}",
            "--progress",
            "--progress-interval", "5s",
            "run",
            "{% if params.dry_run %}--dry-run{% endif %}",
        ],
        mounts=mounts,
        environment={
            **get_db_env_vars(),
            "DATA_DIR": "/state",  # Tell mssql-pg-migrate to use mounted state dir
        },
        docker_url="unix://var/run/docker.sock",
        network_mode=DATABASE_NETWORK,
        auto_remove="success",
        do_xcom_push=True,
        mount_tmp_dir=False,
        timeout=14400,  # 4 hours
        # Retries configured in default_args (3 retries with exponential backoff)
        on_success_callback=on_migration_success,
        on_failure_callback=on_migration_failure,
        on_retry_callback=on_migration_retry,
    )

    # Parse results
    parse_results = PythonOperator(
        task_id="parse_results",
        python_callable=parse_migration_result,
        trigger_rule="all_done",
        on_failure_callback=on_task_failure,
    )

    # Success endpoint
    migration_complete = EmptyOperator(
        task_id="migration_complete",
        trigger_rule="all_success",
    )

    # Define task dependencies
    health_check >> check_health >> [run_migration, health_failed]
    run_migration >> parse_results
    [run_migration, parse_results] >> migration_complete
