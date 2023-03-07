from os.path import expanduser
from pathlib import Path
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from datetime import timedelta


# home = expanduser("~")
# airflow_dir = os.path.join(home, 'airflow')
# assert os.path.isdir(airflow_dir)

# airflow_dir = os.path.join(home, 'airflow')
# Path(f"{airflow_dir}/dags").mkdir(parents=True, exist_ok=True)

default_args = {
    'owner': 'patrickgovus',
    'depends_on_past': False,
    'email': os.environ['EMAIL'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2020, 1, 1),
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2022, 1, 1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'trigger_rule': 'all_success'
}
with DAG(dag_id='update_batch_clean_dataset',
         default_args=default_args,
         schedule_interval='*/1 * * * *',
         catchup=False,
         tags=['test']
         ) as dag:
    # Define the tasks. Here we are going to define only one bash operator
    produce_kafka_messages = BashOperator(
        task_id='produce_kafka_messages',
        bash_command='python3 /Users/paddy/Desktop/pinterest_pipeline/pinterest_API/API/project_pin_API.py',
        dag=dag)
    consume_kafka_messages = BashOperator(
        task_id='consume_kafka_messages',
        bash_command='python3 /Users/paddy/Desktop/pinterest_pipeline/pinterest_API/API/kafka_consumer_batch.py',
        dag=dag)
    spark_cleaning = BashOperator(
        task_id='spark_cleaning',
        bash_command='python3 /Users/paddy/Desktop/pinterest_pipeline/pinterest_API/API/spark_batch.py',
        dag=dag)

    produce_kafka_messages >> consume_kafka_messages >> spark_cleaning