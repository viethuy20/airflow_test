
#!/bin/bash

# Thiết lập AIRFLOW_HOME
export AIRFLOW_HOME="/opt/airflow"

# Thiết lập các biến môi trường
export AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
export AIRFLOW__WEBSERVER__SECRET_KEY="69042ce0acc034ae12b764edb454baa73a2ddc0e861435e2a1f8d749131dc663"
export AIRFLOW__API__AUTH_BACKEND="airflow.api.auth.backend.basic_auth"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export PGPASSWORD="airflow"

echo 'Starting Airflow database initialization...'
echo "AIRFLOW_HOME is set to: $AIRFLOW_HOME"
echo "SQL_ALCHEMY_CONN is set to: $AIRFLOW__CORE__SQL_ALCHEMY_CONN"

# Kiểm tra phiên bản Airflow
echo "Airflow version:"
airflow version

# Đợi PostgreSQL sẵn sàng
echo "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
  if pg_isready -h postgres -p 5432 -U airflow; then
    echo "PostgreSQL is up - proceeding with Airflow setup"
    break
  fi
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done

# Kiểm tra kết nối PostgreSQL
echo "Checking PostgreSQL connection..."
PGPASSWORD=$PGPASSWORD psql -h postgres -U airflow -d airflow -c "SELECT 1" || {
  echo "Error: Cannot connect to PostgreSQL!"
  exit 1
}

# Chạy airflow db migrate
echo "Running airflow db migrate..."
airflow db migrate
if [ $? -ne 0 ]; then
  echo "Error: airflow db migrate failed! Exiting."
  exit 1
fi
echo "Airflow DB migrations complete."

# Tạo người dùng Admin
echo "Creating Airflow admin user..."
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin || {
  echo "Warning: Failed to create admin user. It may already exist or there was an error."
}

echo "Airflow initialization complete!"
exit 0
