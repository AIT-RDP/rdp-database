# Main RDP Database Scheme

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
 * Build the container: `podman build --file docker/Dockerfile --format docker -t rdp-database --label=latest .`
 * Run the container:  `podman run --env-file=.env localhost/rdp-database`

### Extended Setup
Sometimes it is needed to extend the RDP database scheme and add own schema elements. This can be done by creating a
derived project and installing the alembic files as a reference. The following quickstart guide assumes that the basic 
poetry project has already been created.

 * Add the RDP database project as a dependency.
   * Add the package source in the project.toml:
     `poetry source add gitlab-rdp-database https://gitlab-intern.ait.ac.at/api/v4/projects/3005/packages/pypi/simple`
   * Configure the access token (must be done on every fresh installation before calling `poetry install`): 
     `poetry config http-basic.gitlab-rdp-database __token__ <your-deployment-token>`
   * Install the dependency: `poetry add --source gitlab-rdp-database rdp-database`
 * Setup the derived alembic environment. The newly created environment will reference the branches of rdp_db.
   * Initialize the derived alembic installation: `poetry run alembic init --package my_app`
   * Adopt the `alembic.ini` file to your needs (file template, revision file locations, etc.)
   * In the `alembic.ini` file, set the `version_path_separator` to `version_path_separator = ;`. Do not use the `:` 
     character as a version separator. This will be needed to tell alembic that the resource is actually a package.
   * Add the requested version files to `version_locations`. The package names are separated by `:` characters. E.g. 
     `version_locations = my_app/versions;rdp_db:core`
   * Customize the local `env.py` file. Most likely, the logic from `rdp_dp` including the retry logic and various 
     timescale workarounds should be reused. Just replace the entire file content with import 
     `import rdp_db.env` to use the functionality.
   * Do not forget to set the environment variables or to create a proper `.env` file.

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
