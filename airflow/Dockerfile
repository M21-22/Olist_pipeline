FROM apache/airflow:3.3.2-python3.12

USER airflow
COPY airflow/requirements.txt /tmp/olist-airflow-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/olist-airflow-requirements.txt
COPY --chown=airflow:root airflow/include/ /opt/airflow/include/
ENV PYTHONPATH=/opt/airflow

# dbt Core runs in its own environment to avoid conflicts with Airflow providers.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/python -m pip install --no-cache-dir \
       "dbt-core==1.11.17" "dbt-postgres==1.11.0"

# Soda Core 3 imports distutils. Setuptools provides it in Python 3.12.
RUN python -m venv /home/airflow/soda-venv \
    && /home/airflow/soda-venv/bin/python -m pip install --no-cache-dir \
       "soda-core-postgres==3.5.6" "setuptools==84.2.1" \
    && /home/airflow/soda-venv/bin/python -c "from soda.scan import Scan"
RUN mkdir -p /home/airflow/.soda \
    && printf 'send_anonymous_usage_stats: false\n' > /home/airflow/.soda/config.yml
COPY --chown=airflow:root quality/soda/ /opt/airflow/soda/
