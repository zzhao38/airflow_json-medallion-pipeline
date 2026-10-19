FROM apache/airflow:2.11.2

# Only dev/test tooling is added; postgres + common.sql providers ship in the base image.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
