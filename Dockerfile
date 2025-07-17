FROM python:3.13

ENV PYTHONUNBUFFERED 1

WORKDIR /app

#RUN apk add build-base
#RUN apk add python3-dev
#RUN apk add musl-dev
#RUN apk add linux-headers
RUN pip install poetry
COPY pyproject.toml ./
RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi --no-root

COPY ./searcher /app

ENTRYPOINT uvicorn main:app --host 0.0.0.0 --port 9013