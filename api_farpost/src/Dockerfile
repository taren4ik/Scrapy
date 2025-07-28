FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip3 install -r /app/requirements.txt --no-cache-dir

WORKDIR /app

CMD ["uvicorn", "source.main:app",  "--port", "8000", "--host", "0.0.0.0"]

LABEL author='turgenevski@yandex.ru' version=1.0