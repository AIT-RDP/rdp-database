# E³ Database

Hosts the scheme as well as the migration scripts that set up the long-term storage based on Timescale DB. 
The project heavily relies on [Alembic](https://alembic.sqlalchemy.org/en/latest/index.html) to manage DB migration and 
creation.

## Getting started

### Development Setup

 * Create an .env file (e.g. from the example file)
 * Start the database:
   ```podman run --env-file=.env -v e3-timescale-dev:/var/lib/postgresql/data -p 5432:5432 docker.io/timescale/timescaledb:latest-pg14```
 * Create the conda environment: ```conda env create -f environment-dev.yml```
 * Activate environment: ```conda activate e3-database```
 * Run alembic to populate the database: ```alembic upgrade head```

### Productive Setup

## Schema Overview

The database hosts the following main tables:
 * **data_points**: A detailed description of all data points
 * **forecasts**: The actual forecasting time series data including observation and forecasting time stamps
