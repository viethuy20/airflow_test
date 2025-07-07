FROM apache/airflow:2.9.3

USER root
# Cài thêm pyspark và các lib bạn muốn
RUN pip install -r requirements.txt

USER airflow