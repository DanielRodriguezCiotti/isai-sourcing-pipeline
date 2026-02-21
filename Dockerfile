FROM python:3.11-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /data-pipeline/

COPY .python-version /data-pipeline/.python-version
COPY pyproject.toml /data-pipeline/pyproject.toml
COPY uv.lock /data-pipeline/uv.lock

RUN uv sync --only-group base

COPY src /data-pipeline/src
COPY assets /data-pipeline/assets
