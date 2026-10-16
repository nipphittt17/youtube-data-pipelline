FROM apache/airflow:3.3.2

# Airflow + Python dependencies used by the DAGs.
ADD requirements.txt .
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r requirements.txt

# dbt in a separate virtualenv so its dependencies can't conflict with Airflow's.
# DAGs call it as /opt/airflow/dbt_venv/bin/dbt.
ARG DBT_CORE_VERSION=1.12.5
ARG DBT_SNOWFLAKE_VERSION=1.12.1
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
        "dbt-core==${DBT_CORE_VERSION}" \
        "dbt-snowflake==${DBT_SNOWFLAKE_VERSION}"
