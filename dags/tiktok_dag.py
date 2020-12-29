from datetime import timedelta, datetime
import json
import time
import os
import airflow
from urllib.request import urlopen
import pandas as pd
import http.client
import configparser

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import airflow.hooks.S3_hook
import boto3

s3 = boto3.resource('s3')

config = configparser.ConfigParser()
config.read(f"{os.path.expanduser('~')}/airflow/api.config")


def get_api_data():
    print(os.getcwd())
    conn = http.client.HTTPSConnection("tiktok.p.rapidapi.com")

    headers = {
        'x-rapidapi-key': config["rapidapi"]["API_RAPIDAPI_KEY"],
        'x-rapidapi-host': "tiktok.p.rapidapi.com"
        }

    conn.request("GET", "/live/trending/feed", headers=headers)

    res = conn.getresponse()
    data = res.read()
    json_data = json.loads(data.decode("utf-8"))
    return json_data


def get_clean_data(**context):
    video_data = []
    author_data = []
    media = context['task_instance'].xcom_pull(task_ids='get_data', key='return_value').get('media')
    if media:
        for item in media:  
            video_attr = (
            item["video_id"],
            item["create_time"],
            item["description"],
            item["video"]["playAddr"],
            item['statistics']
                         )
            author_attr = (
            item['author']['nickname'], 
            item['author']['uniqueId'],
            item['author']['followers'],
            item['author']['heartCount'],
            item['author']['videoCount']
            )
            video_data.append(video_attr)
            author_data.append(author_attr)
    author_df = pd.DataFrame(author_data, columns=('nickname', 'id', 'followers', 'heartCount', 'videoCount'))
    video_df = pd.DataFrame(video_data, columns=('video_id', 'create_time', 'descriotion', 'playAddr', 'statistics'))
    video_df["create_time"]= pd.to_datetime(video_df['create_time'].apply(lambda x: datetime.fromtimestamp(int(x))))
    video_df.to_csv(f"{os.path.expanduser('~')}/airflow/data/video.csv", index=None)
    author_df.to_csv(f"{os.path.expanduser('~')}/airflow/data/author.csv", index=None)

def upload_file_to_S3_with_hook(filename, key, bucket_name):
    hook = airflow.hooks.S3_hook.S3Hook('aws_default')
    hook.load_file(filename, key, bucket_name)


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5),
    'email': ['airflow@my_first_dag.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


 
with DAG(
    'tiktok_dag',
    default_args=default_args,
    description='Our first DAG',
    schedule_interval="*/2 * * * *",
) as dag:
    get_data = PythonOperator(
        task_id='get_data',
        python_callable=get_api_data,
        dag=dag
)
    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=get_clean_data,
        dag=dag,
        provide_context=True
) 
    
    s3_tasks = []

    for file in [f"{os.path.expanduser('~')}/airflow/data/author.csv", 
                 f"{os.path.expanduser('~')}/airflow/data/video.csv"]:
        upload_to_S3_task = PythonOperator(
                                            task_id=f'upload_to_S3_{file.split("/")[-1]}',
                                            python_callable=upload_file_to_S3_with_hook,
                                            op_kwargs={
                                                'filename': file,
                                                'key': f'{datetime.now().strftime("%Y-%b-%d/%H-%M")}-{file.split("/")[-1]}',
                                                'bucket_name': f'tiktok-fun',
                                            },
                                            dag=dag)

        s3_tasks.append(upload_to_S3_task)
        
    opr_end = BashOperator(task_id='opr_end', bash_command='echo "Done"')
        

get_data >> clean_data >> s3_tasks >> opr_end
