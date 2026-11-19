FROM apache/airflow:3.3.2-python3.12

USER airflow
COPY airflow/requirements.txt /tmp/olist-airflow-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/olist-airflow-requirements.txt
COPY --chown=airflow:root airflow/include/ /opt/airflow/include/
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"

# dbt Core runs in its own environment to avoid conflicts with Airflow providers.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/python -m pip install --no-cache-dir \
       "dbt-core==1.11.17" "dbt-postgres==1.11.0"
