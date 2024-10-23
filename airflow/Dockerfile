FROM apache/airflow:slim-2.10.0-python3.11

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/app"

COPY . .

USER airflow

RUN pip install --upgrade pip

# Установка необходимых зависимостей с правильным синтаксисом
RUN pip install selenium==4.16.0 && \
    pip install beautifulsoup4==4.12.2 && \
    pip install lxml==4.9.2 && \
    pip install psycopg2-binary==2.9.9 && \
    pip install python-dotenv==1.0.0 && \
    pip install apache-airflow-providers-postgres==5.0.0 && \
    pip install numpy
