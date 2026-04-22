# Last.fm Data Pipeline

This project builds an end-to-end music analytics pipeline using the Last.fm API, Apache Airflow, Azure Blob Storage, Azure SQL, and SQLMesh.

The pipeline:

1. Pulls artist and genre-related data from Last.fm.
2. Stores raw extracts as CSV files.
3. Uploads those files to Azure Blob Storage.
4. Loads the raw data into Azure SQL tables.
5. Builds curated gold-layer models with SQLMesh.

## Architecture

The project is organized as a small ELT workflow:

- `Airflow/dags/top_artists.py`
  Downloads the top 1,000 artists from Last.fm and uploads `top_artists.csv` to Azure Blob Storage.
- `Airflow/dags/artist_genres.py`
  Reads the top artists file, fetches a top tag for each artist, and uploads `artist_genres.csv`.
- `Airflow/dags/artists_tracks.py`
  Fetches each artist's top tracks and uploads `artist_tracks.csv`.
- `Airflow/dags/artist_albums.py`
  Fetches each artist's top albums and uploads `artist_albums.csv`.
- `Airflow/dags/artists_geo.py`
  Pulls geo-based top artists for a fixed country list and uploads `geo_top_artists.csv`.
- `Airflow/dags/ingest_to_sql.py`
  Downloads the blob files and inserts them into Azure SQL tables.
- `Airflow/dags/sqlmesh_dag.py`
  Runs SQLMesh to build downstream models in Azure SQL.
- `Airflow/dags/controller.py`
  Orchestrates the full pipeline by triggering the DAGs in sequence.

## Data Flow

`controller_dag` runs the pipeline in this order:

1. `top_artists_dag`
2. `artist_genres_dag`
3. `artist_tracks_dag`
4. `artist_albums_dag`
5. `geo_top_artists_dag`
6. `ingest_to_sql_dag`
7. `sqlmesh_dag`

## Project Structure

```text
.
├── Airflow/
│   ├── dags/
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   └── requirments.txt
├── sqlmesh/
│   ├── config.yaml
│   └── models/
├── top_artists.csv
├── artist_genres.csv
├── artist_tracks.csv
├── artist_albums.csv
└── testing.ipynb
```

## SQLMesh Models

The SQLMesh layer creates curated reporting tables:

- `sqlmesh/models/gold_top_artists.sql`
  Combines global artists, genres, and geo rankings to identify each artist's strongest country and total chart coverage.
- `sqlmesh/models/gold_top_albums.sql`
  Enriches artist albums with genre and calculates album share of artist playcount.
- `sqlmesh/models/gold_top_tracks.sql`
  Enriches artist tracks with genre and calculates track contribution to artist listeners and playcount.
- `sqlmesh/models/gold_geo_artists.sql`
  Adds genre context to country-level artist rankings.

## Requirements

You will need:

- Python 3
- Docker and Docker Compose
- Access to the Last.fm API
- An Azure Blob Storage account/container
- An Azure SQL Database
- ODBC Driver 18 for SQL Server if you run ingestion locally outside Docker

Python dependencies are listed in `Airflow/requirments.txt`:

- `pandas`
- `azure.identity`
- `azure.storage.blob`
- `apache-airflow`
- `pyodbc`
- `sqlmesh`
- `sqlmesh[azuresql]`
- `sqlmesh[airflow]`

## Configuration

The project expects credentials and connection details in Airflow Variables and SQLMesh config.

### Airflow Variables

Create these Airflow Variables before running the DAGs:

- `lastapi`
  Your Last.fm API key.
- `azure_account`
  Azure Blob Storage account URL.
- `container_name`
  Blob container name.
- `properties`
  JSON string containing Azure service principal values:

```json
{
  "tenantId": "...",
  "clientId": "...",
  "client_secret": "..."
}
```

- `sql_server`
- `sql_database`
- `sql_user`
- `sql_password`

### SQLMesh

Update `sqlmesh/config.yaml` with your Azure SQL connection details and preferred gateway settings before running SQLMesh.

## Running The Project

### Option 1: Run with Airflow in Docker

From the `Airflow/` directory:

```bash
docker compose up airflow-init
docker compose up -d
```

Then open Airflow locally at:

```text
http://localhost:8080
```

Trigger `controller_dag` to run the full workflow.

### Option 2: Install Python dependencies locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r Airflow/requirments.txt
```

This is useful for development, debugging, or working with SQLMesh outside the Airflow containers.

## Outputs

### Raw outputs

The pipeline produces these CSV extracts during execution:

- `top_artists.csv`
- `artist_genres.csv`
- `artist_tracks.csv`
- `artist_albums.csv`
- `geo_top_artists.csv`

### Azure SQL tables

The ingestion DAG creates and loads:

- `top_artists`
- `artist_genres`
- `artist_tracks`
- `artist_albums`
- `geo_top_artists`

### Gold models

SQLMesh creates curated tables such as:

- `dbo.gold_top_artists`
- `dbo.gold_top_albums`
- `dbo.gold_top_tracks`
- `dbo.geo_artists`

## Notes

- `Airflow/dags/top_tracks.py` is marked as reference code and is not part of the final orchestration flow.
- The DAGs use local CSV handoffs inside the Airflow environment before uploading to Azure Blob Storage.
- Several folders in this repo, especially `logs/` and `Airflow/logs/`, are runtime artifacts rather than source code.
- For a production version, secrets should be stored in a secure secret manager or Airflow connections/variables rather than committed config files.