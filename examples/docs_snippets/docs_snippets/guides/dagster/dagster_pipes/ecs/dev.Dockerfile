# this Dockerfile can be used for ECS Pipes development

FROM python:3.11-slim

RUN python -m pip install --no-cache dagster-pipes boto3

RUN --mount=type=cache,target=/root/.cache/pip pip install boto3

COPY python_modules/dagster-pipes /src/dagster-pipes

RUN pip install -e /src/dagster-pipes

WORKDIR /app
COPY examples/docs_snippets/docs_snippets/guides/dagster/dagster_pipes/ecs/task.py .
