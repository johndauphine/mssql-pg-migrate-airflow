FROM apache/airflow:3.0.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install required providers for Airflow 3
RUN pip install --no-cache-dir \
    apache-airflow-providers-docker>=3.14.0 \
    apache-airflow-providers-standard>=0.0.1
