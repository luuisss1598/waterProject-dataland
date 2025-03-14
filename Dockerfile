FROM apache/airflow:2.10.5

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY setup.py /opt/airflow/

RUN mkdir -p /opt/airflow/src

RUN pip install -e .