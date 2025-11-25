FROM apache/airflow:2.10.1

ARG AIRFLOW_VERSION=2.10.1
ARG PYTHON_VERSION=3.12

# Speed up pip and reduce cache
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1

COPY requirements.txt /tmp/requirements.txt

# Install with Airflow's official constraints for this base image
RUN python -m pip install --upgrade pip && \
    pip install -r /tmp/requirements.txt \
      --constraint https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt
