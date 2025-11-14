from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from bees_breweries_case.tasks.bronze_extraction_task import (
    BronzeExtractionTask,
)
from bees_breweries_case.tasks.silver_transformation_task import (
    SilverTransformationTask,
)
from bees_breweries_case.tasks.gold_aggregation_task import GoldAggregationTask

from bees_breweries_case.tools.pipeline import Pipeline
from bees_breweries_case.tools.tasks import Tasks

default_args = {
    "start_date": datetime(2024, 1, 1),
    "email": ["giuliamoscoso@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id=Pipeline.PIPELINE_NAME,
    description=Pipeline.PIPELINE_DESCRIPTION,
    schedule_interval="@daily",
    max_active_runs=1,
    default_args=default_args,
    tags=[f"{Pipeline.TAG}"],
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")

    bronze_extraction_task = PythonOperator(
        task_id=Tasks.BRONZE_EXTRACTION_TASK,
        python_callable=BronzeExtractionTask().execute,
        max_active_tis_per_dag=1,
    )

    silver_transformation_task = PythonOperator(
        task_id=Tasks.SILVER_TRANSFORMATION_TASK,
        python_callable=SilverTransformationTask().execute,
        max_active_tis_per_dag=1,
    )

    gold_aggregation_task = PythonOperator(
        task_id=Tasks.GOLD_AGGREGATION_TASK,
        python_callable=GoldAggregationTask().execute,
        max_active_tis_per_dag=1,
    )

    # end task
    end = DummyOperator(task_id="end")

    (
        start
        >> bronze_extraction_task
        >> silver_transformation_task
        >> gold_aggregation_task
        >> end
    )
