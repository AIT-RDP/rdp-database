# E³ Database

Hosts the scheme as well as the migration scripts that set up the long-term storage based on Timescale DB. 
The project heavily relies on [Alembic](https://alembic.sqlalchemy.org/en/latest/index.html) to manage DB migration and 
creation.

## Getting started

### Development Setup

 * Create an .env file (e.g. from the [`docker/.env` file](docker/.env))
 * Start the database:
   ```podman run --env-file=.env -v e3-timescale-dev:/var/lib/postgresql/data -p 5432:5432 docker.io/timescale/timescaledb:latest-pg14```
 * Create the conda environment: ```conda env create -f environment-dev.yml```
 * Activate environment: ```conda activate e3-database```
 * Run alembic to populate the database: ```alembic upgrade head```

### Productive Setup
 * Build the container: `podman build --file docker/Dockerfile --format docker -t e3-database --label=latest .`
 * Run the container:  `podman run --env-file=.env localhost/e3-database`

## Schema Overview

The database hosts the following main tables:
 * **data_points**: A detailed description of all data points
 * **forecasts**: The actual forecasting time series data including observation and forecasting time stamps
 * **measurements**: The time-series collecting real-time measurements only

### Security Concept
The scheme uses Row Level Security (RLS) on the **data_points** table to separate the data that is visible to certain 
user groups as indicated by the `view_role` column. For performance reasons, the raw **forecasts** and **measurements**
tables do not implement such mechanisms and must be protected from unauthorized access. To expose the information to
corresponding view roles, dedicates views that filter the information are provided. The following graphics shows the
hierarchy of roles:

![Main DB Roles](docs/db-roles.png)

One grouping role, `data_source_base` manages full access to the raw tables as well as corresponding sequences and
indices to group all data sources. Since data sources may also update rows, the corresponding role also grants view
permissions on the relevant forecast and measurement tables.  In contrast to the data source roles, `view_base` groups
all roles that can retrieve data but may not store new forecasts and measurements. The data view groups `view_internal`
and `view_public` are directly referenced in the **data_points** table and control the exposure of data points. Login
users can be assigned to one or multiple of these grouping roles to expose the corresponding time series data. Since 
views inherit the permissions of the owner, a dedicated role, `restricting_view_executor` is introduced that has 
access to the raw data and triggers the RLS policies.
