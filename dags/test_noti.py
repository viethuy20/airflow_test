from __future__ import annotations
from connect_db.connect_sql import connect_to_db, query_db
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.decorators import task, dag

DAG_DOC = """
This is an example DAG that runs a simple Python function and a Bash command.
"""
from airflow.hooks.base import BaseHook
import smtplib
from email.mime.text import MIMEText

def send_mail(context, success=True):
    # Lấy connection từ Airflow
    conn = BaseHook.get_connection("smtp_gmail")
    smtp_server = conn.host  # Lấy host (smtp.gmail.com)
    smtp_port = conn.port    # Lấy port (587)
    login = conn.login       # Lấy login (tranviethuy197@gmail.com)
    password = conn.password # Lấy password (App Password)

    # Tạo email
    msg = MIMEText("Đây là email test từ scheduler với connection Airflow.")
    msg['Subject'] = "Test Email từ Scheduler với Airflow Connection"
    msg['From'] = login  # Lấy email từ connection
    msg['To'] = "vn-huy-tv@login.gmo-ap.jp"

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(login, password)
        server.sendmail(msg['From'], msg['To'], msg.as_string())
        print("Kết nối và gửi email thành công")
        server.quit()
    except Exception as e:
        print(f"Lỗi: {e}")

@dag(
    dag_id="example_dagxxx",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    doc_md=DAG_DOC,
)
def example_dag():
    @task
    def print_hello():
        print("Hello, World!")

    @task(on_success_callback=send_mail)
    def get_data():
        con = connect_to_db()
        print("conn", con)
        if con:
            query = "SELECT * FROM actor LIMIT 5;"
            results = query_db(query)
            return results
        else:
            print("Failed to connect to the data.")

    @task
    def print_goodbye():
        print("Goodbye, World!")


    print_hello_task = print_hello()
    connect_to_db_task = get_data()
    print_goodbye_task = print_goodbye()

    print_hello_task >> connect_to_db_task >> print_goodbye_task

test_dag = example_dag()