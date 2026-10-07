from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from src.extract.extract import extract_data, save_to_s3
from src.load.load import read_from_s3, load_to_postgres


@dag(
    dag_id="coingecko_pipeline",
    start_date=datetime(2026, 9, 29),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    }
)
def coingecko_pipeline():

    @task
    def extract():
        data = extract_data()
        save_to_s3(data)

    transform = GlueJobOperator(
        task_id="transform",
        job_name="coingecko-transform",
        wait_for_completion=True,
        region_name="eu-west-1",
    )

    @task
    def load():
        df = read_from_s3()
        load_to_postgres(df)

    extract_task = extract()
    load_task = load()

    extract_task >> transform >> load_task


coingecko_pipeline()