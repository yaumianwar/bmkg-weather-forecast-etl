FROM apache/airflow:2.10.5
USER root
RUN apt-get update && apt-get install -y \
    liblz4-dev \
    build-essential  # Required for compiling C/C++ extensions
USER airflow


ADD requirements.txt .
RUN pip install --upgrade pip setuptools wheel
RUN pip install airflow-provider-clickhouse
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt