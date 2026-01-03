"""
Database Migration DAG using mssql-pg-migrate

This DAG orchestrates database migrations between MSSQL and PostgreSQL
using the mssql-pg-migrate tool via DockerOperator.

Features:
- Health check before migration
- Automatic retry with resume on failure
- Streaming progress via --progress flag
- Proper exit code handling for recoverable vs non-recoverable errors
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException
from docker.types import Mount
import json
import os


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
}

RECOVERABLE_CODES = {2, 5, 7}

# Network where databases are running
DATABASE_NETWORK = os.getenv("DATABASE_NETWORK", "mssql-to-postgres-pipeline_airflow")

# Config mount path - override via environment variable for different hosts
CONFIG_MOUNT_PATH = os.getenv("CONFIG_MOUNT_PATH", "/Users/john/repos/mssql-pg-migrate-airflow/configs")

# State directory for checkpoints (must be shared between retries)
STATE_MOUNT_PATH = os.getenv("STATE_MOUNT_PATH", "/Users/john/repos/mssql-pg-migrate-airflow/state")


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

        return super().execute(context)


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
        ],
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
            "DATA_DIR": "/state",  # Tell mssql-pg-migrate to use mounted state dir
        },
        docker_url="unix://var/run/docker.sock",
        network_mode=DATABASE_NETWORK,
        auto_remove="success",
        do_xcom_push=True,
        mount_tmp_dir=False,
        timeout=14400,  # 4 hours
        # Retries configured in default_args (3 retries with exponential backoff)
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
    run_migration >> parse_results >> migration_complete
