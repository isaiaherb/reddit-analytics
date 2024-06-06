FROM apache/airflow:2.9.1

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow

WORKDIR /app

ADD requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt

COPY dags /opt/airflow/dags

USER root
RUN chown -R airflow: /opt/airflow/dags
USER airflow
