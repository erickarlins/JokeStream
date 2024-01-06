import json
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

default_args = {
    'owner': 'Karlins',
    'start_date': datetime(2024, 1, 4, 10, 00)
}

def get_data():
    #res = requests.get("https://randomuser.me/api/")
    res = requests.get("https://geek-jokes.sameerkumar.website/api?format=json")
    res = res.json()
    res = res['joke']
    return res

def format_data(res):
    data = {}
    data['joke'] = res
    return data

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('jokes_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue


with DAG('user_automation',
         default_args=default_args,
         schedule_interval = '@daily',
         catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable = stream_data
    )

