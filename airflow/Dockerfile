FROM apache/airflow:slim-2.11.0-python3.10

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt