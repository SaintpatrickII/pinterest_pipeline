from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'patrickgovus',
    'depends_on_past': False,
    'email': 'patrickgovus99@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': datetime(2020, 2, 2),

}
with DAG(dag_id='update_batch_clean_dataset',
         default_args=default_args,
         schedule_interval='0 12 * * *',
         catchup=False,
         dagrun_timeout=timedelta(minutes=60),
         tags=['test']
         ) as dag:
  
    sleep2 = BashOperator(
        task_id="sleep2",
        bash_command="sleep 30",
        dag=dag)
    consume_kafka_messages = BashOperator(
        task_id='consume_kafka_messages',
        bash_command='python3 /Users/paddy/Desktop/pinterest_pipeline/pinterest_API/API/kafka_consumer_batch.py',
        dag=dag)
    sleep3 = BashOperator(
        task_id="sleep3",
        bash_command="sleep 5",
        retries=3,
    )
    spark_cleaning = BashOperator(
        task_id='spark_cleaning',
        bash_command='python3 /Users/paddy/Desktop/pinterest_pipeline/pinterest_API/API/spark_batch.py',
        dag=dag)

sleep2 >> consume_kafka_messages >> sleep3 >> spark_cleaning 