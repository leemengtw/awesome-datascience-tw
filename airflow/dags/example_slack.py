import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.slack_operator import SlackAPIPostOperator


default_args = {
    'owner': 'Meng Lee',
    'start_date': datetime(2018, 7, 17, 0, 0),
    'schedule_interval': '@yearly',
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'concurrency': 1,
    'max_active_runs': 1
}


def get_token():
    file_dir = os.path.dirname(__file__)
    token_path = os.path.join(file_dir, '../data/credentials/slack.json')
    with open(token_path, 'r') as fp:
        token = json.load(fp)['token']
        return token


with DAG('example_slack', default_args=default_args) as dag:

    send_message = SlackAPIPostOperator(
        task_id='send_message',
        token=get_token(),
        channel='#your-channel-name',
        text="Message from Airflow!",
        icon_url='http://airbnb.io/img/projects/airflow3.png'
    )

