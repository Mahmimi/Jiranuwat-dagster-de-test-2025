## Project Overview

This project handles the ETL process, reading data from Excel/CSV files and loading it into DuckDB with data type transformations. It includes logging with Dagster and a DataFrame preview for debugging.

### Features
- ETL for Excel/CSV data to DuckDB.
- Logging with Dagster.
- Data type casting and validation.
- DataFrame preview in Dagster UI.

### Fixes in Version 1.0.0
- Handled string-to-number conversion issues.
- Fixed unclosed database connections after data load.

### Version 1.0.0
![Screenshot](version_image/version1.0.0.PNG)

# Dagster Data Engineering Test

Follow the below instructions to complete this test.

## Requirements
- Python 3.12 or higher
- Docker (If installation is not possible, use GitHub Codespaces for development.)
- uv package management (Installation: [uv on PyPI](https://pypi.org/project/uv/))
- DuckDB
  - via CLI: [Installation Guide](https://duckdb.org/docs/installation/?version=stable&environment=cli&platform=macos&download_method=package_manager)  
  - recommended via IDE: [DBeaver Guide](https://duckdb.org/docs/stable/guides/sql_editors/dbeaver.html)  

## Getting Started

**Fork** the GitHub repository and name it `<Firstname>-dagster-de-test-2025`:
- Go to [https://github.com/sidataplus/dagster-de-test-2025](https://github.com/sidataplus/dagster-de-test-2025)
- Click the **Fork** button at the top-right to create a copy of this repository under your GitHub account
- Then **Clone** the forked project to your local machine:
```bash
git clone https://github.com/sidataplus/dagster-de-test-2025.git
cd dagster-de-test-2025
```

via Docker:
```bash
docker compose build
docker compose up --watch # Auto rebuild on changes
```
> **Note:** To reload code after making changes, go to the "Deployment" tab and click "Reload All."

There may be some error messages initially, but it should stabilize within a few seconds once all services are up and running.

You should be able to access the Dagster UI at http://localhost:3000.

> In case of using GitHub Codespaces, after starting your Dagster instance, open the "Ports" tab in the GitHub Codespaces interface. You should see a forwarded port 3000. Click on the link to access the Dagster UI directly.

## Adding New Packages

In case you want to add additional dependencies, run:
```bash
uv add <Package name>
```

Then run:
```bash
uv lock  # Updates dependencies without upgrading existing ones
# or
uv lock --upgrade  # To upgrade and update the lock file
uv sync  # To install from the lock file
```