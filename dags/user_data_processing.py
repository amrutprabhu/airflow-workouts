from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

from datetime import datetime
import json

default_args = {
    'start_date': datetime(2020, 1, 1)
}


def _processing_user(ti):
    users_txt = ti.xcom_pull(task_ids=["fetch_user"])[0]
    users = json.loads(users_txt)
    if not len(users) or 'results' not in users:
        raise ValueError("User is empty")
    user = users['results'][0]
    user_map = {
        'firstname': user['name']['first'],
        'lastname': user['name']['last']
    }
    processed_user = json.dumps(user_map)
    Variable.set("user", processed_user)


with DAG('user_data_processing',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )

    fetch_user = SimpleHttpOperator(
        task_id='fetch_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET'
    )

    processing_user = PythonOperator(
        task_id='processing_user',
        python_callable=_processing_user
    )

    print_user = BashOperator(
        task_id='log_user',
        bash_command='echo "{{ var.value.user }}  {{ params.customer_key }}"',
        params={"customer_key": "value"}
    )

    is_api_available >> fetch_user >> processing_user >> print_user
