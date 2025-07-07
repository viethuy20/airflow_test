from __future__ import annotations
from connect_db.connect_sql import connect_to_db , query_db
import pendulum
import sys
import os
from pyspark_file.connect_pyspark import create_dataframe
from airflow.providers.slack.operators.slack import SlackAPIOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.email import EmailOperator
from airflow.utils.email import send_email
import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Connection
DAG_DOC = """
This is an example DAG that runs a simple Python function and a Bash command.   """
SLACK_API_CONN_ID = 'slack_webhook_conn'
from airflow.decorators import task, dag

def send_notifi(context,success=True):

    slack_notification = SlackWebhookOperator(
        task_id='slack_webhook_notification',
        slack_webhook_conn_id=SLACK_API_CONN_ID, # ID kết nối webhook trong Airflow
        message='This is a notification from Airflow DAG: ran successfully.', # Sử dụng 'message' thay vì 'text'
        channel='#test12', # Tên kênh Slack thực tế
        username='Airflow API Bot',
        icon_emoji=':airflow:'
    )
    # Hàm execute cần một context để hoạt động đúng cách
    return slack_notification.execute(context)

@dag(
    dag_id = "example_dag",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    doc_md=DAG_DOC,
)
def example_dag():
    @task
    def print_hello():
        print('heloo',sys.path)
        
    @task()
    def get_data():
        con = connect_to_db()
        print("conn",con)
        if con:
            query = "SELECT * FROM actor LIMIT 5;"
            results, col = query_db(query)
            df = create_dataframe(data=results, columns=col)
            return df
            
            # con.close()
        else:
            return pd.DataFrame()  # Trả về DataFrame rỗng nếu không kết nối được
            
    @task
    def print_goodbye():
        print("Goodbye, World!")
        
    # email_notification = EmailOperator(
    #     task_id='email_notification',
    #     to='vn-huy-tv@login.gmo-ap.jp',
    #     subject='Airflow DAG Notification',
    #     html_content='This is a notification from Airflow DAG: ran successfully.',
    #     conn_id='smtp_gmail',
    # )
    
    @task(on_success_callback=send_notifi, on_failure_callback=send_notifi)
    def upload_to_minio(results):
        """
        `results` là pandas.DataFrame
        """
        import pandas as pd
        import boto3
        print("Uploading results to MinIO...", results)
        # 1. Lưu DataFrame ra file CSV tạm
        filename = "/tmp/results.csv"
        results.to_csv(filename, index=False)

        # 2. Tải lên MinIO/S3
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin123",
            region_name="us-east-1",
        )
        bucket = "your-bucket-name"
        object_name = "results.csv"

        # Kiểm tra tạo bucket nếu cần
        try:
            s3.head_bucket(Bucket=bucket)
        except:
            s3.create_bucket(Bucket=bucket)

        s3.upload_file(filename, bucket, object_name)
        print(f"Đã upload {object_name} lên MinIO bucket {bucket}!")

    print_hello_task = print_hello()
    connect_to_db_task = get_data()
    print_goodbye_task = print_goodbye()
    upload_to_minio_task = upload_to_minio(connect_to_db_task)

    print_hello_task  >> connect_to_db_task >> upload_to_minio_task >> print_goodbye_task

test_dag = example_dag()