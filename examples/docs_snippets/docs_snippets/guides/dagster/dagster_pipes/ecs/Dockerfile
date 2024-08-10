FROM python:3.11-slim

RUN python -m pip install --no-cache dagster-pipes boto3

# TODO: replace this with `pip install dagster-pipes` once the docs are finished
RUN pip install --no-cache-dir dagster-pipes boto3

WORKDIR /app

COPY task.py .
