FROM apache/airflow:3.3.2

ARG AIRFLOW_VERSION=3.3.2
COPY requirements.txt /tmp/requirements.txt
# Install the extra providers/libs under Airflow's official constraints so nothing in the base
# image gets bumped to an incompatible version.
RUN PYTHON_MM="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" \
 && pip install --no-cache-dir -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MM}.txt"
