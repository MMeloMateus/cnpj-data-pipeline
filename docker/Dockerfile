FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

COPY requirements.txt /opt/project/requirements.txt

RUN apt-get update \
  && apt-get install -y --no-install-recommends gcc libpq-dev build-essential \
  && pip install --no-cache-dir -r /opt/project/requirements.txt \
  && apt-get remove -y gcc build-essential \
  && apt-get autoremove -y \
  && rm -rf /var/lib/apt/lists/*

COPY airflow /opt/project/airflow

# Criação das Var Ambiente
ENV AIRFLOW_HOME=/opt/airflow_home
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/project/airflow/dags
ENV PYTHONPATH=/opt/project/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

WORKDIR /opt/project/airflow

# Inicializar banco e criar usuário admin
RUN airflow db init \
  && airflow users create \
     --username admin \
     --firstname Admin \
     --lastname User \
     --role Admin \
     --email admin@example.com \
     --password admin

CMD ["airflow", "standalone"]