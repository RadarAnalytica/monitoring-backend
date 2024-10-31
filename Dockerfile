FROM python:3.12-bookworm

ENV PYTHONUNBUFFERED 1

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN pip install poetry
RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi

COPY ./searcher /app

ENTRYPOINT python setup.py && uvicorn main:app --workers 2 --host 0.0.0.0 --port 9013