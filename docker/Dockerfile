FROM python:3.10-alpine

WORKDIR /usr/src/app

COPY requirements.txt alembic.ini docker/.env ./
COPY e3_ems ./e3_ems

RUN pip install --no-cache-dir -r requirements.txt

CMD ["alembic", "upgrade", "head"]