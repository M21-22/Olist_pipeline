FROM apache/airflow:3.3.2-python3.12

USER airflow
COPY airflow/requirements.txt /tmp/olist-airflow-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/olist-airflow-requirements.txt
COPY --chown=airflow:root airflow/include/ /opt/airflow/include/
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"
