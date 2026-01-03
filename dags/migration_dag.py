"""
Database Migration DAG using mssql-pg-migrate

This DAG orchestrates database migrations between MSSQL and PostgreSQL
using the mssql-pg-migrate tool via DockerOperator.

Features:
- Health check before migration
- Streaming progress via --progress flag
- Proper exit code handling
- JSON output for structured results
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from docker.types import Mount
import json


# Default arguments for all tasks
default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=4),
}

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
DATABASE_NETWORK = "mssql-to-postgres-pipeline_airflow"


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


with DAG(
    dag_id="mssql_pg_migration",
    default_args=default_args,
    description="Migrate data from MSSQL to PostgreSQL",
    schedule_interval=None,  # Manual trigger only
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
                source="/home/johnd/repos/mssql-pg-migrate-airflow/configs",
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

    # Run the actual migration
    run_migration = DockerOperator(
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
                source="/home/johnd/repos/mssql-pg-migrate-airflow/configs",
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
        # Timeout after 4 hours
        timeout=14400,
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


# Separate DAG for resuming failed migrations
with DAG(
    dag_id="mssql_pg_migration_resume",
    default_args=default_args,
    description="Resume a failed/interrupted migration",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["migration", "mssql", "postgres", "resume"],
    params={
        "config_file": "test-migration.yaml",
    },
) as resume_dag:

    resume_migration = DockerOperator(
        task_id="resume_migration",
        image="mssql-pg-migrate:1.24.0",
        command=[
            "--config", "/config/{{ params.config_file }}",
            "--progress",
            "--progress-interval", "5s",
            "resume",
        ],
        mounts=[
            Mount(
                source="/home/johnd/repos/mssql-pg-migrate-airflow/configs",
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
    )
