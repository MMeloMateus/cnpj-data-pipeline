FROM python:3.11-slim

# Evitar prompts interativos e reduzir tamanho
ENV DEBIAN_FRONTEND=noninteractive

# Workdir para organização da BUILD
WORKDIR /opt/project

COPY airflow/requirements.txt /opt/project/requirements.txt

RUN apt-get update \
  && apt-get install -y --no-install-recommends gcc libpq-dev build-essential \
  && pip install --no-cache-dir -r /opt/project/requirements.txt \
  && apt-get remove -y gcc build-essential \
  && apt-get autoremove -y \
  && rm -rf /var/lib/apt/lists/*

# Cópiar os códigos da aplicação
COPY airflow /opt/project/airflow
COPY dbt /opt/project/dbt

ENV AIRFLOW_HOME=/opt/project/airflow
ENV PYTHONPATH=/opt/project/airflow

# Workdir para aplicação
WORKDIR /opt/project/airflow

CMD ["airflow", "standalone"]