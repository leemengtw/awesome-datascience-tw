import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Meng Lee',
    'start_date': datetime(2018, 8, 12, 0, 0),
    'schedule_interval': '@yearly',
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}


def demo_macro_usage(**context):

    print(kwargs['ds'])
    macros = kwargs['macros']
    print(macros.ds_add(kwargs['ds'], -6))


with DAG(DAG_NAME, default_args=default_args, schedule_interval='@daily',
         concurrency=10, max_active_runs=1) as dag:
    t1 = PythonOperator(
        task_id='demo_macro_usage',
        python_callable=demo_macro_usage,
        provide_context=True
    )