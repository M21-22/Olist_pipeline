import logging
import socket
from datetime import datetime

from airflow.sdk import dag, task

logger = logging.getLogger(__name__)


@dag(
    dag_id="infrastructure_smoke_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["infrastructure", "smoke-test"],
)
def infrastructure_smoke_test():
    @task(queue="default")
    def test_default_queue():
        hostname = socket.gethostname()
        logger.info("DEFAULT queue task executed on %s", hostname)
        return hostname

    @task(queue="ingest")
    def test_ingest_queue():
        hostname = socket.gethostname()
        logger.info("INGEST queue task executed on %s", hostname)
        return hostname

    @task(queue="batch")
    def test_batch_queue():
        hostname = socket.gethostname()
        logger.info("BATCH queue task executed on %s", hostname)
        return hostname

    default = test_default_queue()
    ingest = test_ingest_queue()
    batch = test_batch_queue()

    [default, ingest] >> batch


infrastructure_smoke_test()
