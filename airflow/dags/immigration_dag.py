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
    description='Create Immigration Data analytics schema',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['udacity'],
) as dag:

    extract_clean_immigration = SparkOperator(
        task_id='extract_clean_immigration',
        script_file="extract_clean_immigration.py"
    )

    extract_clean_cities = SparkOperator(
        task_id='extract_clean_cities',
        script_file="extract_clean_cities.py"
    )

    extract_clean_airports = SparkOperator(
        task_id='extract_clean_airports',
        script_file="extract_clean_airports.py"
    )

    sync_after_extract = DummyOperator(
        task_id='sync_after_extract'
    )

    create_dim_person = SparkOperator(
        task_id='create_dim_person',
        script_file="create_dim_person.py"
    )

    create_dim_destination = SparkOperator(
        task_id='create_dim_destination',
        script_file="create_dim_destination.py"
    )

    create_dim_flight = SparkOperator(
        task_id='create_dim_flight',
        script_file="create_dim_flight.py"
    )

    create_dim_airport = SparkOperator(
        task_id='create_dim_airport',
        script_file="create_dim_airport.py"
    )

    create_fact_immigration = SparkOperator(
        task_id='create_fact_immigration',
        script_file="create_fact_immigration.py"
    )

    data_quality_check = SparkOperator(
        task_id='data_quality_check',
        script_file="data_quality_check.py"
    )

    # extract_and_transform_raw_files.doc_md = dedent(
    #     """\
    # #### Task Documentation
    # You can document your task using the attributes `doc_md` (markdown),
    # `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    # rendered in the UI's Task Instance Details page.
    # ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    # """
    # )

    # dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
    # dag.doc_md = """
    # This is a documentation placed anywhere
    # """  # otherwise, type it like this

    [extract_clean_cities, extract_clean_immigration,
        extract_clean_airports] >> sync_after_extract
    sync_after_extract >> [create_fact_immigration, create_dim_person,
                           create_dim_airport, create_dim_destination, create_dim_flight] >> data_quality_check
