FROM apache/airflow:2.4.1-python3.10
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/app"

COPY . /app

USER airflow
RUN pip install selenium && \
    pip install bs4 && \
    pip install lxml && \
    pip install selenium-stealth