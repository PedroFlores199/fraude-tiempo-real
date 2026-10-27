FROM apache/airflow:2.10.3-python3.11

COPY requirements/airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.3" -r /tmp/requirements-airflow.txt
