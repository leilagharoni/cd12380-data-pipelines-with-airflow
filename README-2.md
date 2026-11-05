# Sparkify Data Pipeline with Apache Airflow

This project loads Sparkify event logs and song metadata from Amazon S3 into Amazon Redshift. Custom Airflow operators stage the JSON data, populate a songplays fact table and four dimension tables, and run SQL data quality checks.

## Environment and current configuration

- Airflow 2.8.1 runs locally through Docker Compose on Windows.
- Amazon Redshift Serverless and the S3 bucket are in `us-west-2`.
- The database is `dev`.
- The DAG ID is `final_project`.
- The DAG is scheduled hourly and is kept paused during manual testing.
- Events now use a logical-date template to select one daily file.
- Songs currently use `song-data/A/A/`, a smaller subset used for the successful test.

The original full song-data COPY was canceled after a prolonged run without confirmed file-loading progress. A successful load of the entire song dataset has not been verified.

## Project files

| File                                    | Purpose                                                                |
| --------------------------------------- | ---------------------------------------------------------------------- |
| `dags/final_project.py`               | DAG configuration, task parameters, dependencies and quality checks    |
| `plugins/operators/stage_redshift.py` | Stages JSON files from S3 with Redshift COPY                           |
| `plugins/operators/load_fact.py`      | Inserts fact records without truncating the fact table                 |
| `plugins/operators/load_dimension.py` | Loads dimensions using configurable truncate-and-insert or append mode |
| `plugins/operators/data_quality.py`   | Compares SQL query results with expected values                        |
| `plugins/helpers/sql_queries.py`      | Fact and dimension transformations                                     |
| `create_tables.sql`                   | Creates the staging, fact and dimension tables                         |
| `docker-compose.yaml`                 | Local Airflow, PostgreSQL and Redis services                           |

The `airflow1/` folder belongs to the legacy starter and is not used by this setup.

## Pipeline flow

1. Begin execution.
2. Stage events and songs in parallel.
3. Load the songplays fact table after both staging tasks succeed.
4. Load users, songs, artists and time dimensions in parallel.
5. Run data quality checks after all dimension tasks succeed.
6. Stop execution.

The DAG has 10 tasks. It does not depend on past runs, retries failed tasks three times with a five-minute delay, disables catchup and does not send retry emails.

## Operator behavior and transformations

The staging operator templates `s3_key`, retrieves AWS credentials from an Airflow connection, supports temporary session tokens, and executes COPY using SQL parameters. DELETE and COPY execute in one transaction so a failed load can roll back the staging changes. Credentials are not explicitly written to the task log.

The fact operator appends records. The fact SQL excludes null user IDs and uses a `NOT EXISTS` condition to avoid inserting play IDs already present in the fact table.

Dimension tasks currently use `truncate=True`. The users query selects the latest event for each user within the current staging data. The artists query uses `ROW_NUMBER()` to select one record per artist ID with a deterministic ordering; this is not a latest-artist-record rule. The time query selects distinct timestamps.

With daily event staging, the users dimension is a snapshot of users in that daily file. This setup does not implement historical user-dimension merges.

## Local setup

Install and start Docker Desktop with Linux containers. Open a PowerShell terminal in the project directory.

Create a `.env` file beside `docker-compose.yaml`:

```text
AIRFLOW_UID=50000
```

Initialize Airflow, then start its services:

```powershell
docker compose up airflow-init
docker compose up -d
```

Open [http://localhost:8080](http://localhost:8080). The starter configuration creates the local account with username `airflow` and password `airflow`.

Docker mounts the local `dags/` and `plugins/` directories into the Airflow containers. Save code changes locally and allow the scheduler time to parse them.

## Airflow connections

Under **Admin > Connections**, configure the following.

### Redshift

| Field           | Value                                                           |
| --------------- | --------------------------------------------------------------- |
| Connection Id   | `redshift`                                                    |
| Connection Type | Postgres                                                        |
| Host            | Your Redshift Serverless endpoint, without a URL scheme or port |
| Database        | `dev`                                                         |
| Login           | Your Redshift database username                                 |
| Password        | Your Redshift database password                                 |
| Port            | `5439`                                                        |
| Extra           | `{"sslmode":"require","connect_timeout":10}`                  |

For this local setup, public access was enabled on the workgroup and its security group permits TCP port 5439 from the local computer's public IP using a /32 rule. Update that rule if the public IP changes.

### AWS

| Field                            | Value                          |
| -------------------------------- | ------------------------------ |
| Connection Id                    | `aws_credentials`            |
| Connection Type                  | Amazon Web Services            |
| Login / AWS Access Key ID        | Current Udacity lab access key |
| Password / AWS Secret Access Key | Current Udacity lab secret key |

For temporary lab credentials, set Extra to:

```json
{
  "region_name": "us-west-2",
  "aws_session_token": "**********"
}
```

Refresh all temporary credentials when the lab session changes or they expire. Do not commit credentials or connection exports.

## S3 inputs

The configured bucket is `sparkify-airflow-787569976096-1793737086`.

| Input              | Key or URI                                                           |
| ------------------ | -------------------------------------------------------------------- |
| Event logs         | `log-data/YYYY/MM/YYYY-MM-DD-events.json`                          |
| Tested song subset | `song-data/A/A/`                                                   |
| Full song prefix   | `song-data/`                                                       |
| Event JSON mapping | `s3://sparkify-airflow-787569976096-1793737086/log_json_path.json` |

Events use the supplied JSON mapping. Songs use JSON `auto`.

## Create the tables

Run this once before the initial load. The SQL script drops and recreates all seven project tables, removing any existing project data.

```powershell
docker compose cp create_tables.sql airflow-worker:/tmp/create_tables.sql
docker compose exec airflow-worker python -c "from airflow.providers.postgres.hooks.postgres import PostgresHook; PostgresHook(postgres_conn_id='redshift').run(open('/tmp/create_tables.sql').read(), split_statements=True); print('Tables created')"
```

## Date-based loading

The events task uses:

```python
s3_key="log-data/{{ logical_date.strftime('%Y/%m/%Y-%m-%d') }}-events.json",
```

Template rendering was verified without executing a data load:

```powershell
docker compose exec airflow-worker airflow tasks render final_project Stage_events 2018-11-01
```

Rendered key:

```text
log-data/2018/11/2018-11-01-events.json
```

To run the current DAG, choose a logical date with an existing input file, such as November 1, 2018. A manual run using today's logical date would select a missing event file.

Keep the DAG paused until connections, input paths and tables are ready. Unpause it and trigger one run with the intended historical logical date. Use Graph view and task logs to inspect the result.

The hourly project schedule can be restored by setting `schedule_interval='0 * * * *'`. Keep the DAG paused after restoring it: the current inputs are historical daily files, and enabling a present-day hourly schedule would select files that are not available.

## Validation results

An end-to-end run succeeded using all November event logs from`log-data/` and the song subset `song-data/A/A/`.

After updating the staging operator to execute SQL through the Airflow hook, daily event staging for November 1, 2018 was tested successfully
and loaded 15 rows.

The table below shows results from the earlier full-November event run,including the corrected artists dimension. The current `staging_events`
table contains 15 rows following the daily staging test.



After fixing duplicate artist IDs and rebuilding the artists dimension, the following counts describe the validated data:

| Table          |  Rows |
| -------------- | ----: |
| staging_events | 8,056 |
| staging_songs  |   604 |
| songplays      | 6,820 |
| users          |    96 |
| songs          |   604 |
| artists        |   587 |
| time           | 6,813 |

The updated quality task passed all 15 checks: each final table is nonempty, each primary key has no nulls, and each primary key has no duplicates.

To test only the quality task against existing data:

```powershell
docker compose exec airflow-worker airflow tasks test final_project Run_data_quality_checks 2026-11-04
```

This task does not reload S3 data. The test date above identifies the task test; the quality SQL checks the current table contents.

Unmatched song or artist IDs in songplays can remain null because the fact query uses a left join and the tested song input is only a subset. The checks above do not establish complete song matching or full-dataset coverage.

## Troubleshooting and shutdown

A live-log request returned HTTP 403 during testing, while local log files remained visible. That log-serving issue is separate from the COPY result and has not been corrected in this setup.

Stop local services when finished:

```powershell
docker compose down
```

This preserves the named PostgreSQL volume. Stopping Docker does not delete or stop AWS resources. Monitor the Udacity lab budget and clean up AWS resources when they are no longer needed. The configured monthly 10 RPU-hour compute limit is not a dollar cap and does not cover all storage or S3 charges.
