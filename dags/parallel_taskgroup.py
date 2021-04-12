from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup

import json
from subdags.subdag_parallel_dag import subdag_parallel_dag

default_args = {
    'start_date': datetime(2020, 1, 1)
}


with DAG('parallel_task_group',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

        task_1 = BashOperator(
            task_id='task_1',
            bash_command="sleep 3"
        )
        
        with TaskGroup("processing_task") as processing_task:
            task_2 = BashOperator(
                task_id ='task_2',
                bash_command="sleep 3"
            )

            with TaskGroup("Spark_task") as Spark_task:
                task_2 = BashOperator(
                    task_id ='task_2',
                    bash_command="sleep 3"
                )

            with TaskGroup("flink_task") as flink_task:

                task_3 = BashOperator(
                    task_id ='task_3',
                    bash_command="sleep 3"
                )
        
        task_4 = BashOperator(
            task_id ='task_4',
            bash_command="sleep 3"
        )

        task_1 >> processing_task >> task_4
