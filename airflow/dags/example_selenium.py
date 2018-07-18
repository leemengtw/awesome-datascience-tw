import time
import logging
from datetime import datetime, timedelta
from selenium import webdriver
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Meng Lee',
    'start_date': datetime(2018, 7, 17, 0, 0),
    'schedule_interval': '@yearly',
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}


def access_website(**kwargs):
    driver = webdriver.Chrome()
    start_url = "http://www.google.com"
    driver.get(start_url)
    logging.info("Got the page!")
    time.sleep(10)
    driver.quit()


with DAG('example_selenium', default_args=default_args) as dag:
    selenium = PythonOperator(
        task_id='access_website',
        python_callable=access_website,
        provide_context=True
    )