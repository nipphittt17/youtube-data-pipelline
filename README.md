# YouTube Data Pipeline

An end-to-end batch data pipeline that captures daily snapshots of YouTube channel and video statistics using the YouTube Data API. Raw JSON data is stored in Amazon S3, loaded into Snowflake staging tables, and transformed into analytics-ready data marts using dbt. Apache Airflow orchestrates the workflow, automating data ingestion, loading, and transformation.

## Data pipeline
[To be added]

## Tech stack

| Layer | Tool |
| --- | --- |
| Source | YouTube Data API v3 |
| Object Storage | Amazon S3 (key-pair auth)|
| Warehouse | Snowflake (key-pair auth) |
| Transformation | dbt Core |
| Orchestration | Apache Airflow 3.3 (Docker Compose) |

## How it works

### 1. Ingestion: `youtube_daily_ingestion` (runs `@daily`)

Defined in [`dags/youtube_daily_ingestion.py`](dags/youtube_daily_ingestion.py). It tracks channels, whose IDs are set in `.env`.

```
load_channel_s3 ──────────────────────────────► load_channel_snowflake ─► Asset: stg_channel_daily_snapshot
load_video_s3 ──► load_video_detail_s3 ───────► load_video_snowflake   ─► Asset: stg_video_daily_snapshot
```

| Task | What it does | Output |
| --- | --- | --- |
| `load_channel_s3` | Calls `channels.list` for each channel to get its metadata and total views, subscribers, and videos. | `s3://<bucket>/channel-snapshot/cs-<YYYYMMDD>-<channel_id>.json` |
| `load_video_s3` | Finds each channel's uploads playlist and pages through `playlistItems.list`.| `s3://<bucket>/playlist-snapshot/ps-<YYYYMMDD>-<channel_id>.json` |
| `load_video_detail_s3` | Reads the playlist files and calls `videos.list` in batches to get each video's details and statistics. | `s3://<bucket>/video-snapshot/vs-<YYYYMMDD>-<channel_id>.json` |
| `load_channel_snowflake` | Reads channel files from s3 [channel-snapshot], convert it to DataFrame, and runs a `MERGE` into the staging table. | `STAGING.STG_CHANNEL_DAILY_SNAPSHOT` |
| `load_video_snowflake` | Does the same for the video files [video-snapshot]. | `STAGING.STG_VIDEO_DAILY_SNAPSHOT` |

### 2. Transformation: `youtube_transformation` (triggered by Assets)

Defined in [`dags/youtube_transformation.py`](dags/youtube_transformation.py). This DAG is [Asset-scheduled](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html). It runs only after both staging tables have been updated (`CHANNEL_SNAPSHOT_ASSET & VIDEO_SNAPSHOT_ASSET`, declared in [`dags/youtube_assets.py`](dags/youtube_assets.py)).

```
dbt_debug ──► dbt_build   (runs the models and their tests)
```
#### dbt model: `dbt_youtube_transformation/`

The models are written to create tables in the `MART` schema. They take tables in the `STAGING` shcema (`STG_CHANNEL_DAILY_SNAPSHOT` and `STG_CHANNEL_DAILY_SNAPSHOT` ) as inputs.

| Model | Grain | Description |
| --- | --- | --- |
| `dim_channel` | 1 row per channel | Latest channel attributes (title, handle, publish date). |
| `dim_video` | 1 row per video | Latest video attributes (title, description, tags, category, duration, definition, captions, thumbnail). |
| `fct_channel_daily_performance` | channel × day | Running totals plus day-over-day growth (absolute and %) in views, subscribers, and videos. |
| `fct_video_daily_performance` | video × day | Running totals plus day-over-day growth in views, likes, and comments, the like/comment/interaction-to-view ratios, and `video_age_days`. |

Tests in [`models/schema.yml`](dbt_youtube_transformation/models/schema.yml) check `not_null` and `unique` on the dimension keys, and `dbt_utils.unique_combination_of_columns` on the fact table grains.

## Project structure

```
youtube-data-pipeline/
├── dags/
│   ├── youtube_assets.py         
│   ├── youtube_daily_ingestion.py 
│   └── youtube_transformation.py   
├── dbt_youtube_transformation/
│   ├── dbt_project.yml
│   ├── macros/generate_schema_name.sql
│   └── models/
│       ├── schema.yml                 
│       └── mart/                      
├── Dockerfile                      
├── docker-compose.yaml               
└── requirements.txt
```

## Getting started

### Prerequisites

- Docker and Docker Compose (at least 4 GB of RAM for Docker)
- A Google Cloud project with **YouTube Data API v3** enabled, and an API key
- An AWS account with an S3 bucket, and credentials in `~/.aws`. For example, run `aws configure` or `aws login`.
- A Snowflake account with a user set up for [key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth)

### 1. Create the Snowflake objects
The ingestion DAG merges into staging tables that must already exist. 

```sql
CREATE DATABASE IF NOT EXISTS TADA_YOUTUBE_DATA;
CREATE SCHEMA IF NOT EXISTS TADA_YOUTUBE_DATA.STAGING;
CREATE SCHEMA IF NOT EXISTS TADA_YOUTUBE_DATA.MART;

CREATE TABLE IF NOT EXISTS TADA_YOUTUBE_DATA.STAGING.STG_CHANNEL_DAILY_SNAPSHOT (
    RECORDED_AT      DATE,
    CHANNEL_ID       VARCHAR,
    USERNAME         VARCHAR,
    CHANNEL_TITLE    VARCHAR,
    PUBLISHED_AT     TIMESTAMP_TZ,
    TOTAL_VIEW       NUMBER,
    TOTAL_SUBSCRIBER NUMBER,
    TOTAL_VIDEO      NUMBER
);

CREATE TABLE IF NOT EXISTS TADA_YOUTUBE_DATA.STAGING.STG_VIDEO_DAILY_SNAPSHOT (
    RECORDED_AT          DATE,
    VDO_ID               VARCHAR,
    VDO_TITLE            VARCHAR,
    PUBLISHED_AT         TIMESTAMP_TZ,
    VDO_DESCRIPTION      VARCHAR,
    VDO_CATEGORY_ID      VARCHAR,
    VDO_TAGS             ARRAY,
    VDO_DEFAULT_LANGUAGE VARCHAR,
    CHANNEL_ID           VARCHAR,
    THUMBNAILS_URL       VARCHAR,
    VDO_DURATION         VARCHAR,
    VDO_DEFINITION       VARCHAR,
    VDO_CAPTION          VARCHAR,
    VDO_VIEW_CNT         NUMBER,
    VDO_LIKE_CNT         NUMBER,
    VDO_COMMENT_CNT      NUMBER
);
```

### 2. Set up AWS IAM permission
Add this permission to the allowed IAM users
```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "AllowS3BucketListing",
			"Effect": "Allow",
			"Action": "s3:ListBucket",
			"Resource": "arn:aws:s3:::{bucket_name}"
		},
		{
			"Sid": "AllowS3ObjectAccess",
			"Effect": "Allow",
			"Action": [
				"s3:GetObject",
				"s3:PutObject"
			],
			"Resource": "arn:aws:s3:::{bucket_name}/*"
		}
	]
}
```

### 3. Set up secrets

Create a `.env` file in the project root.
```dotenv
# YouTube
API_KEY=<youtube-data-api-key>
DICE_CHANNEL_ID=<channel-id>
BUS_CHANNEL_ID=<channel-id>
TADA_CHANNEL_ID=<channel-id>

# AWS
S3_BUCKET=<bucket-name>
AWS_DEFAULT_REGION=ap-southeast-2

# Snowflake (key-pair auth)
SNOWFLAKE_ACCOUNT=<account-identifier>
SNOWFLAKE_USER=<user>
SNOWFLAKE_ROLE=<role>
SNOWFLAKE_WAREHOUSE=<warehouse>
SNOWFLAKE_DATABASE=TADA_YOUTUBE_DATA
SNOWFLAKE_SCHEMA=STAGING
SNOWFLAKE_PRIVATE_KEY_FILE=/opt/airflow/keys/rsa_key.p8
SNOWFLAKE_PRIVATE_KEY_PWD=<passphrase-if-any>

```

Docker Compose mounts these local files into the containers:

| Local path | Mounted at | Used by |
| --- | --- | --- |
| `./rsa_key.p8` | `/opt/airflow/keys/rsa_key.p8` | Snowpark loader (Snowflake private key) |
| `~/.aws/` | `/home/airflow/.aws` | `boto3` (S3 credentials) |
| `~/.dbt/profiles.yml` | `/opt/airflow/.dbt/profiles.yml` | dbt |

Example `~/.dbt/profiles.yml`:

```yaml
dbt_youtube_transformation:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <account-identifier>
      user: <user>
      role: <role>
      authenticator: snowflake_jwt
      private_key_path: /opt/airflow/keys/rsa_key.p8
      private_key_passphrase: <passphrase-if-any>
      warehouse: <warehouse>
      database: TADA_YOUTUBE_DATA
      schema: MART
      threads: 4
```

### 4. Build and start Airflow

```bash
docker compose up airflow-init
docker compose up -d --build
docker compose exec airflow-scheduler \
  bash -c "cd /opt/airflow/dbt_youtube_transformation && /opt/airflow/dbt_venv/bin/dbt deps"   # install dbt_utils
```
Open <http://localhost:8080> and log in with `airflow` / `airflow`.

## Analytics usage
[To be added]

## Possible next steps
[To be added]
