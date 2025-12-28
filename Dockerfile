FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

COPY requirements.txt /opt/project/requirements.txt

RUN apt-get update \
  && apt-get install -y --no-install-recommends gcc libpq-dev build-essential \
  && pip install --no-cache-dir -r /opt/project/requirements.txt \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Copiar o projeto
COPY airflow /opt/project/airflow

# Variáveis de ambiente
ENV AIRFLOW_HOME=/opt/project/airflow
ENV PYTHONPATH=/opt/project/airflow

WORKDIR /opt/project/airflow
EXPOSE 8080

# Entrypoint padrão
ENTRYPOINT ["bash", "-c"]