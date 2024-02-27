# Do not use the alpine images as it is missing many dependencies
FROM ${DOCKER_PROXY_PREFIX}python:3.10 as builder

COPY requirements-dev.txt ./
RUN pip install -r requirements-dev.txt

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN touch README.md

RUN poetry install --without dev --no-root && \
    rm -rf $POETRY_CACHE_DIR

# Build the runtime image
FROM ${DOCKER_PROXY_PREFIX}python:3.10-slim

# Use the environment variables instead of arguments to specify the config and .env file. This should make both
# customization and invoking subcommands easier. Additionally, the venc is set without having to use poetry
ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY alembic.ini docker/.env ./
COPY rdp_db ./rdp_db

CMD ["alembic", "upgrade", "head"]