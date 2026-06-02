# Submarine Telemetry Analytics Pipeline

Submarine Telemetry Analytics Pipeline is a telemetry analytics pipeline for underwater vehicle run data. The project takes generated or provided telemetry, converts it into Parquet, stores it in AWS S3, defines analytical views in Athena, and displays the results through Grafana dashboards.

The current pipeline is:

`synthetic telemetry / CSV input -> Parquet -> S3 -> Glue Data Catalog / Athena -> Grafana`

## System Architecture

This project is organized around moving submarine telemetry into AWS so it can be queried with Athena and visualized in Grafana.

Telemetry files enter the project through local repository folders. CSV inputs are automatically converted into Parquet, and synthetic telemetry is generated as Parquet directly. All Parquet telemetry files are collected in:

`data/parquet_out/`

An AWS webhook detects files placed in this location and uploads them to the project S3 bucket. Athena reads the uploaded Parquet files through the `sensor_data` external table, and Grafana queries Athena views built on top of that table.

High-level flow:

```text
Synthetic data / CSV input
        |
        v
CSV-to-Parquet conversion
        |
        v
Local Parquet drop folder
        |
        v
AWS S3
        |
        v
Glue Data Catalog / Athena external table
        |
        v
Athena analytical views
        |
        v
Grafana dashboards
```
TODO: Replace with architecture diagram or pipeline graphic

## Repository Structure

```text
.
├── archive/
├── config/
├── data/
├── docs/
├── scripts/
├── sql/
├── src/
├── README.md
└── pyproject.toml
```

`scripts/` contains runnable project scripts, such as data generation and CSV-to-Parquet conversion.

`sql/` contains the Athena SQL files used to create or recreate the project’s tables and views.

TODO: Add missing SQL files from saved queries on AWS

`docs/` contains additional documentation, including a user guide, schema information and query descriptions.

`data/` contains the Parquet telemetry files that are uploaded to the S3 bucket and queried through Athena.

`archive/` contains older or deprecated files that were part of previous versions of the project, such as the original DuckDB workflow and earlier CSV/Parquet conversion scripts.

## Data Model / Schema

The main Athena table is:

`sensor_data`

Each row represents one telemetry sample from a run.

Key identifier columns:

```text
timestamp_utc
seq
run_id
```

Main sensor columns include:

```text
battery_voltage_v
battery_current_a
depth_m
humidity_pct
imu_ax_mps2
imu_ay_mps2
imu_az_mps2
```

For the full schema, see:

`docs/schema.md`

## AWS Setup

The project uses AWS S3 to store Parquet telemetry data.

Telemetry Parquet files are stored in S3 under:

`s3://senior-project-data-370852768002-us-east-1-an/curated/`

Athena reads the Parquet data through an external table named `sensor_data`. The table metadata is stored in the Glue Data Catalog, allowing Athena to treat the files in S3 as a queryable table.

Main Athena database:

`senior_project`

Main Athena table:

`sensor_data`

Athena views are defined on top of `sensor_data` and are used as the query layer for Grafana panels.

Athena query results are written to:

`s3://senior-project-data-370852768002-us-east-1-an/athena-results/`

TODO: Add screenshot of S3 bucket/prefix structure.

TODO: Add screenshot of Athena table/schema.

## Athena SQL Views

The project stores Athena SQL definitions in the repository so the views can be recreated if needed. This prevents the query logic from existing only inside the AWS console.

The SQL files are located in:

`sql/`

The main Athena views include:

### Energy Consumption Per Run

Estimates total battery energy usage for each run in watt-hours using current and voltage telemetry. The output is grouped by `run_id`.

### Dive / Depth Summary

Uses `depth_m` over time to summarize depth behavior for each run, including maximum depth, average depth, vertical movement rate, and time spent near maximum depth.

### Voltage Sag Events

Finds points where voltage drops while current increases. This is used to identify possible high-load moments in the battery data.

### IMU Motion Anomaly Detection

Uses acceleration data from the IMU to estimate motion intensity and flag unusually large deviations from recent motion behavior.

### Phase Segmentation

Labels telemetry rows as `surface`, `descending`, `holding_depth`, or `ascending` based on depth and vertical movement rate. This supports phase-based Grafana panels such as time spent per phase and average current/voltage per phase.

TODO: Add remaining SQL views, like Z score

## Grafana Dashboards

Grafana connects to Athena as its data source. When a dashboard is opened or refreshed, Grafana runs SQL queries against Athena views. Those Athena views analyze the telemetry stored in S3 and return the results used by the dashboard panels.

Current notable dashboard panels include:

TODO: update these to actually say the interesting panels 

- Energy-per-run comparison
- Depth profile per run
- Time spent per phase
- Average current and voltage per phase
- Voltage sag events
- Motion anomaly summaries

TODO: Add screenshot of Grafana dashboard overview.

TODO: Add screenshot of one important Grafana panel close-up.

## How to Use

Telemetry data enters the pipeline through the local data folders. The project supports three input paths:

1. Add an existing Parquet telemetry file to `data/parquet_out/`.
2. Generate synthetic telemetry using the provided generator. Generated runs are written to `data/parquet_out/` automatically.
3. Add a telemetry CSV file to `data/csv/`. CSV files are automatically converted to Parquet and written to `data/parquet_out/`.

After Parquet files are added to `data/parquet_out/`, the AWS webhook uploads them to S3. Open or refresh the Grafana dashboard to query the updated data through Athena.

Grafana connects to Athena as its data source. When a dashboard is opened or refreshed, Grafana runs SQL queries against Athena views. Those Athena views analyze the telemetry stored in S3 and return the results used by the dashboard panels.

## How to Reproduce

Start by adding telemetry data using one of the three accepted input paths described above. Once Parquet files are available in `data/parquet_out/`, the AWS webhook uploads them to the S3 `curated/` prefix.

Create the main Athena external table by running:

`sql/make_sensor_data.sql`

This creates the `sensor_data` table over the Parquet telemetry files stored in S3.

After `sensor_data` exists, run the remaining SQL files in `sql/` to create or recreate the Athena views.

Open or refresh the Grafana dashboard. Grafana must already be connected to Athena as a data source. Once connected, it queries the Athena views and displays the updated telemetry analysis.

For more detailed operating instructions, see the user guide:

`docs/UserGuide.md`

## Notes

- The current telemetry data is synthetic and does not represent real vehicle telemetry.
- Athena views are stored SQL definitions and recompute when queried.
- Grafana is used for visualization; analysis logic lives in Athena SQL views.
- Deprecated DuckDB files and older scripts are retained in `archive/` for reference, but are not part of the current pipeline.

