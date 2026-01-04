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

# Config mount path - override via environment variable for different hosts
CONFIG_MOUNT_PATH = os.getenv("CONFIG_MOUNT_PATH", "/Users/john/repos/mssql-pg-migrate-airflow/configs")

# State directory for checkpoints (must be shared between retries)
STATE_MOUNT_PATH = os.getenv("STATE_MOUNT_PATH", "/Users/john/repos/mssql-pg-migrate-airflow/state")

# Database credentials (passed to mssql-pg-migrate container for config file substitution)
DB_ENV_VARS = {
    "MSSQL_PASSWORD": os.getenv("MSSQL_PASSWORD", ""),
    "PG_PASSWORD": os.getenv("PG_PASSWORD", ""),
}

# Slack webhook URL for notifications
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def send_slack_notification(status: str, config_file: str, dag_id: str = "mssql_pg_migration",
                           run_id: str = None, start_time: str = None, details: dict = None, error: str = None):
    """Send Slack notification with migration details."""
    if not SLACK_WEBHOOK_URL:
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
        speed = details.get("speed", 0)
        summary = f"Migration pipeline completed successfully. Migrated {tables} tables with {rows:,} total rows. Throughput: {speed:,} rows/sec."
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

    # Add error block if present and not already in summary
    if error and status == "FAILED":
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Error Details:*\n```{error}```"}
        })

    payload = {"blocks": blocks}

    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to send Slack notification: {e}")


def parse_migration_output(output: str) -> dict:
    """Parse migration output to extract details."""
    details = {}
    if not output:
        return details

    # Look for the final completion line
    for line in output.split("\n"):
        # "Migration complete: 2 tables, 1401431 rows in 2s (785105 rows/sec)"
        match = re.search(r"Migration complete: (\d+) tables?, ([\d,]+) rows? in ([^\(]+) \((\d+) rows/sec\)", line)
        if match:
            details["tables"] = int(match.group(1))
            details["rows"] = int(match.group(2).replace(",", ""))
            details["duration"] = match.group(3).strip()
            details["speed"] = int(match.group(4))
            break

        # Also check JSON progress for completed status
        if line.startswith("{"):
            try:
                progress = json.loads(line)
                if progress.get("phase") == "completed":
                    details["tables"] = progress.get("tables_complete", 0)
                    details["rows"] = progress.get("rows_transferred", 0)
                    details["speed"] = progress.get("rows_per_second", 0)
            except json.JSONDecodeError:
                pass

    return details


def on_migration_success(context):
    """Callback for successful migration."""
    ti = context["ti"]
    dag_run = context["dag_run"]
    config_file = context["params"].get("config_file", "unknown")
    output = ti.xcom_pull(task_ids="run_migration")
    details = parse_migration_output(output)

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


def on_migration_failure(context):
    """Callback for failed migration."""
    ti = context["ti"]
    dag_run = context["dag_run"]
    config_file = context["params"].get("config_file", "unknown")
    exception = context.get("exception")

    # Build detailed error message
    if exception:
        error_msg = str(exception)
        # Check for exit code in the exception
        match = re.search(r"StatusCode['\"]?:\s*(\d+)", error_msg)
        if match:
            exit_code = int(match.group(1))
            exit_desc = EXIT_CODES.get(exit_code, f"Exit code {exit_code}")
            error_msg = f"{exit_desc} (exit code {exit_code})"
    else:
        error_msg = "Task failed"

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


def on_migration_retry(context):
    """Callback for migration retry."""
    ti = context["ti"]
    dag_run = context["dag_run"]
    config_file = context["params"].get("config_file", "unknown")
    exception = context.get("exception")

    # Build detailed error message
    if exception:
        error_msg = str(exception)
        # Check for exit code in the exception
        match = re.search(r"StatusCode['\"]?:\s*(\d+)", error_msg)
        if match:
            exit_code = int(match.group(1))
            exit_desc = EXIT_CODES.get(exit_code, f"Exit code {exit_code}")
            error_msg = f"{exit_desc} (exit code {exit_code})"
    else:
        error_msg = "Task interrupted"

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
    health_check = DockerOperator(
        task_id="health_check",
        image="mssql-pg-migrate:1.24.0",
        command=[
            "--config", "/config/{{ params.config_file }}",
            "health-check",
        ],
        mounts=[
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
        ],
        environment=DB_ENV_VARS,
        docker_url="unix://var/run/docker.sock",
        network_mode=DATABASE_NETWORK,
        auto_remove="success",
        do_xcom_push=True,
        mount_tmp_dir=False,
        retries=2,  # Health check can retry on transient network issues
    )

    # Branch based on health check
    check_health = BranchPythonOperator(
        task_id="check_health",
        python_callable=check_health_result,
    )

    # Health check failed - stop pipeline
    health_failed = EmptyOperator(
        task_id="health_check_failed",
    )

    # Run the actual migration (with automatic resume on retry)
    run_migration = MigrationDockerOperator(
        task_id="run_migration",
        image="mssql-pg-migrate:1.24.0",
        command=[
            "--config", "/config/{{ params.config_file }}",
            "--progress",
            "--progress-interval", "5s",
            "run",
            "{% if params.dry_run %}--dry-run{% endif %}",
        ],
        mounts=[
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
        ],
        environment={
            **DB_ENV_VARS,
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
