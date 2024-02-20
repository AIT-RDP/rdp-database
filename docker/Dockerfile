FROM python:3.10-alpine

WORKDIR /usr/src/app

COPY requirements.txt alembic.ini docker/.env ./
COPY rdp_db ./rdp_db

RUN pip install --no-cache-dir -r requirements.txt

CMD ["alembic", "upgrade", "head"]