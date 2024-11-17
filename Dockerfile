FROM python:3.12-alpine

ENV PYTHONUNBUFFERED 1

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN apk add build-base
RUN apk add python3-dev
RUN apk add musl-dev
RUN apk add linux-headers
RUN pip install poetry
RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi

COPY ./searcher /app

ENTRYPOINT python setup.py && uvicorn main:app --host 0.0.0.0 --port 9013