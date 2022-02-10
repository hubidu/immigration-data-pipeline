from datetime import datetime, timedelta
from textwrap import dedent
from venv import create

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from custom_operator.spark_operator import SparkOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'immigration_pipeline',
    default_args=default_args,
    description='Create immigration analytics schema from i94 immigration data, us cities and airports',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['udacity'],
) as dag:

    extract_clean_immigration = SparkOperator(
        task_id='extract_clean_immigration',
        script_file="extract_clean_immigration.py"
    )
    extract_clean_immigration.doc_md = dedent("""
    Extract and clean the I94 dataset and store the results in s3.
    """)

    extract_clean_cities = SparkOperator(
        task_id='extract_clean_cities',
        script_file="extract_clean_cities.py"
    )
    extract_clean_cities.doc_md = dedent("""
    Extract and clean the us cities dataset and store the results in s3.
    """)

    extract_clean_airports = SparkOperator(
        task_id='extract_clean_airports',
        script_file="extract_clean_airports.py"
    )
    extract_clean_airports.doc_md = dedent("""
    Extract and clean the airports dataset and store the results in s3.
    """)

    sync_after_extract = DummyOperator(
        task_id='sync_after_extract'
    )
    sync_after_extract.doc_md = dedent("""
    Synchronize the extract tasks
    """)

    create_dim_person = SparkOperator(
        task_id='create_dim_person',
        script_file="create_dim_person.py"
    )
    create_dim_person.doc_md = dedent("""
    Create the person dimension table from intermediary files and store results in s3.
    """)

    create_dim_destination = SparkOperator(
        task_id='create_dim_destination',
        script_file="create_dim_destination.py"
    )
    create_dim_destination.doc_md = dedent("""
    Create the destination dimension table from intermediary files and store results in s3.
    """)

    create_dim_flight = SparkOperator(
        task_id='create_dim_flight',
        script_file="create_dim_flight.py"
    )
    create_dim_flight.doc_md = dedent("""
    Create the flight dimension table from intermediary files and store results in s3.
    """)

    create_dim_airport = SparkOperator(
        task_id='create_dim_airport',
        script_file="create_dim_airport.py"
    )
    create_dim_airport.doc_md = dedent("""
    Create the airport dimension table from intermediary files and store results in s3.
    """)

    create_fact_immigration = SparkOperator(
        task_id='create_fact_immigration',
        script_file="create_fact_immigration.py"
    )
    create_fact_immigration.doc_md = dedent("""
    Create the immigration fact table from intermediary files and store results in s3.
    """)

    data_quality_check = SparkOperator(
        task_id='data_quality_check',
        script_file="data_quality_check.py"
    )
    data_quality_check.doc_md = dedent("""
    Load the final data sets and run some data checks on them.
    """)

    [extract_clean_cities, extract_clean_immigration,
        extract_clean_airports] >> sync_after_extract
    sync_after_extract >> [create_fact_immigration, create_dim_person,
                           create_dim_airport, create_dim_destination, create_dim_flight] >> data_quality_check
