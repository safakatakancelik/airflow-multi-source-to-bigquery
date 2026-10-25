ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}

ARG AIRFLOW_VERSION
COPY requirements.txt /tmp/requirements.txt
# Install the extra providers and libraries under Airflow's official constraints so nothing in the
# base image gets bumped to an incompatible version.
RUN PYTHON_MM="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" \
 && pip install --no-cache-dir -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MM}.txt"
