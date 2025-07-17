FROM python:3.13-alpine

ENV PYTHONUNBUFFERED=1
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app

RUN apk add --no-cache \
    build-base \
    python3-dev \
    musl-dev \
    linux-headers \
    libffi-dev \
    openssl-dev \
    curl \
    git

RUN curl -sSL https://install.python-poetry.org | python3 -

COPY pyproject.toml ./
RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi --no-root

COPY ./searcher /app

ENTRYPOINT ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9013"]