FROM apache/airflow:2.9.3
# Chuyển sang user root để có quyền cài đặt các gói hệ thống
USER root

# Cập nhật danh sách gói và cài đặt Java Runtime Environment (JRE)
# Sau đó dọn dẹp để giữ cho image có kích thước nhỏ
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk \
    iputils-ping \
    curl \
    telnet \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường JAVA_HOME để PySpark có thể tìm thấy Java
# ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt