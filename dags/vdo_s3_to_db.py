import json
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


# Keep imports of third-party data libraries inside task functions.
# Airflow parses this file frequently; importing Polars and Snowpark at
# module level can make DAG parsing slow or hit the import timeout.


def s3_setup():
    import boto3

    region = os.getenv(
            "AWS_DEFAULT_REGION",
            "ap-southeast-2",
        )
    bucket = os.environ["S3_BUCKET"]

    s3 = boto3.client(
        "s3",
        region_name=region,
    )

    return bucket, s3


def load_folder_s3(s3_prefix, s3_date_prefix):
    bucket, s3 = s3_setup()
    paginator = s3.get_paginator("list_objects_v2")

    json_data = []

    target_prefix = f"{s3_prefix}{s3_date_prefix}"

    for page in paginator.paginate(
            Bucket=bucket,
            Prefix=target_prefix,
        ):

        for obj in page.get("Contents", []):
            s3_key = obj["Key"]
            filename = os.path.basename(s3_key)

            if not filename.startswith(f"{s3_date_prefix}-"):
                continue
            if not filename.endswith(".json"):
                continue

            response = s3.get_object(
                Bucket=bucket,
                Key=s3_key,
            )

            try:
                body = response["Body"].read().decode("utf-8")
                json_data.append(json.loads(body))
                print(f"Loaded: {s3_key}")
            finally:
                response["Body"].close()

    print(f"Loaded {len(json_data)} JSON files from {target_prefix}.")
    return json_data


def extract_items(data, prefix):
    """Normalize a list, YouTube API response, or single object into a list."""
    if isinstance(data, list):
        return data
    
    if isinstance(data, dict):
        items = data.get("items")
        return items if isinstance(items, list) else [data]

    print(f"Skipping unexpected JSON structure: {prefix}")
    return []


def process_channel_data(run_date, s3_date_prefix):
    import polars as pl

    channels = []
    FOLDERS = [
        "youtube-dice/",
        "youtube-sonray/",
    ]

    for prefix in FOLDERS:

        for data in load_folder_s3(prefix, s3_date_prefix):

            for channel in extract_items(data, prefix):

                if not isinstance(channel, dict):
                    continue

                stats = channel.get("statistics", {})
                snippet = channel.get("snippet", {})

                channels.append({
                    "RECORDED_AT": run_date,
                    "USERNAME": snippet.get("customUrl"),
                    "TITLE": snippet.get("title"),
                    "PUBLISHED_AT": snippet.get("publishedAt"),

                    "TOTAL_VIEW": (
                        int(stats["viewCount"])
                        if stats.get("viewCount") is not None
                        else None
                    ),

                    "TOTAL_SUBSCRIBER": (
                        int(stats["subscriberCount"])
                        if stats.get("subscriberCount") is not None
                        else None
                    ),

                    "TOTAL_VIDEO": (
                        int(stats["videoCount"])
                        if stats.get("videoCount") is not None
                        else None
                    ),
                })

    if not channels:
        return pl.DataFrame()

    channel_df = (
        pl.DataFrame(channels)
        .sort(["RECORDED_AT", "USERNAME"], descending=True)
        .unique(
            subset=["RECORDED_AT", "USERNAME"],
            keep="first",
            maintain_order=True,
        )
    )
    print(f"Processed {channel_df.height} channel records.")
    print(channel_df)
    return channel_df

def process_video_data(run_date, s3_date_prefix):
    import polars as pl

    videos = []
    folders = [
        "youtube-video-details-dice/",
        "youtube-video-details-sonray/",
    ]

    for prefix in folders:
        for data in load_folder_s3(prefix, s3_date_prefix):
            for video in extract_items(data, prefix):
                if not isinstance(video, dict):
                    continue

                stats = video.get("statistics") or {}
                snippet = video.get("snippet") or {}
                contents = video.get("contentDetails") or {}
                thumbnails = snippet.get("thumbnails") or {}
                default_thumbnail = thumbnails.get("default") or {}

                videos.append({
                    "RECORDED_AT": run_date,
                    "VDO_ID": video.get("id"),
                    "VDO_TITLE": snippet.get("title"),
                    "PUBLISHED_AT": snippet.get("publishedAt"),
                    "CHANNEL_ID": snippet.get("channelId"),
                    "CHANNEL_ID": snippet.get("channelId"),
                    "CHANNEL_TITLE": snippet.get("channelTitle"),
                    "VDO_CATEGORY_ID": snippet.get("categoryId"),
                    "VDO_TAGS": snippet.get("tags"),
                    "VDO_DEFAUlT_LANGUAGE": snippet.get("defaultAudioLanguage"),

                    "THUMBNAILS_URL": default_thumbnail.get("url"),
                    "THUMBNAILS_WIDTH": default_thumbnail.get("width"),
                    "THUMBNAILS_HEIGHT": default_thumbnail.get("height"),


                    "VDO_DURATION": contents.get("duration"),
                    "VDO_DEFINITION": contents.get("definition"),
                    "VDO_CAPTION": contents.get("caption"),

                    "VDO_VIEW_CNT": int(stats["viewCount"]) if stats.get("viewCount") is not None else None,
                    # YouTube video statistics do not normally contain subscriberCount.
                    "VDO_LIKE_CNT": int(stats["likeCount"]) if stats.get("likeCount") is not None else None,
                    "VDO_COMMENT_CNT": int(stats["commentCount"]) if stats.get("commentCount") is not None else None,
                })

    if not videos:
        return pl.DataFrame()

    video_df = pl.DataFrame(videos)
    if "VDO_ID" in video_df.columns:
        video_df = video_df.unique(
            subset=["RECORDED_AT", "VDO_ID"],
            keep="first",
            maintain_order=True,
        )

    print(f"Processed {video_df.height} video records.")
    print(video_df)
    return video_df

def get_snowpark_session():
    from snowflake.snowpark import Session

    connection_parameters = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"],
    }

    private_key_password = os.getenv("SNOWFLAKE_PRIVATE_KEY_PWD")

    if private_key_password:
        connection_parameters["private_key_file_pwd"] = private_key_password

    return Session.builder.configs(connection_parameters).create()


def ingest_channel_to_snowflake():
    from snowflake.snowpark.functions import when_matched, when_not_matched

    run_date = datetime.now()
    s3_date_prefix = run_date.strftime("%Y%m%d")
    channel_df = process_channel_data(run_date, s3_date_prefix)

    if channel_df.is_empty():
        print("No channel data found. Skipping ingestion.")
        return

    session = get_snowpark_session()

    try:
        records = channel_df.to_dicts()
        target_table = session.table("CHANNEL_SNAPSHOT")
        source_df = session.create_dataframe(records)

        join_condition = (
            (target_table["USERNAME"] == source_df["USERNAME"])
            & (target_table["RECORDED_AT"] == source_df["RECORDED_AT"])
        )

        target_table.merge(
            source=source_df,
            join_expr=join_condition,
            clauses=[
                when_matched().update({
                    "TITLE": source_df["TITLE"],
                    "PUBLISHED_AT": source_df["PUBLISHED_AT"],
                    "TOTAL_VIEW": source_df["TOTAL_VIEW"],
                    "TOTAL_SUBSCRIBER": source_df["TOTAL_SUBSCRIBER"],
                    "TOTAL_VIDEO": source_df["TOTAL_VIDEO"],
                }),
                when_not_matched().insert({
                    "RECORDED_AT": source_df["RECORDED_AT"],
                    "USERNAME": source_df["USERNAME"],
                    "TITLE": source_df["TITLE"],
                    "PUBLISHED_AT": source_df["PUBLISHED_AT"],
                    "TOTAL_VIEW": source_df["TOTAL_VIEW"],
                    "TOTAL_SUBSCRIBER": source_df["TOTAL_SUBSCRIBER"],
                    "TOTAL_VIDEO": source_df["TOTAL_VIDEO"],
                }),
            ],
        )

        print(f"Merged {len(records)} rows into CHANNEL_SNAPSHOT.")

    finally:
        session.close()

def ingest_video_to_snowflake():
    from snowflake.snowpark.functions import when_matched, when_not_matched

    run_date = datetime.now()
    s3_date_prefix = run_date.strftime("%Y%m%d")
    video_df = process_video_data(run_date, s3_date_prefix)

    if video_df.is_empty():
        print("No video data found. Skipping ingestion.")
        return

    session = get_snowpark_session()

    try:
        records = video_df.to_dicts()
        target_table = session.table("VIDEO_SNAPSHOT")
        source_df = session.create_dataframe(records)

        join_condition = (
            (target_table["VDO_ID"] == source_df["VDO_ID"])
            & (target_table["RECORDED_AT"] == source_df["RECORDED_AT"])
        )

        target_table.merge(
            source=source_df,
            join_expr=join_condition,
            clauses=[
                when_matched().update({
                    "RECORDED_AT": run_date,
                    "VDO_TITLE": source_df["VDO_TITLE"],
                    "PUBLISHED_AT": source_df["PUBLISHED_AT"],
                    "CHANNEL_ID": source_df["CHANNEL_ID"],
                    "CHANNEL_TITLE": source_df["CHANNEL_TITLE"],
                    "VDO_CATEGORY_ID": source_df["VDO_CATEGORY_ID"],
                    "VDO_TAGS": source_df["VDO_TAGS"],
                    "VDO_DEFAUlT_LANGUAGE": source_df["VDO_DEFAUlT_LANGUAGE"],

                    "THUMBNAILS_URL": source_df["THUMBNAILS_URL"],
                    "THUMBNAILS_WIDTH": source_df["THUMBNAILS_WIDTH"],
                    "THUMBNAILS_HEIGHT": source_df["THUMBNAILS_HEIGHT"],

                    "VDO_DURATION": source_df["VDO_DURATION"],
                    "VDO_DEFINITION": source_df["VDO_DEFINITION"],
                    "VDO_CAPTION": source_df["VDO_CAPTION"],

                    "VDO_VIEW_CNT": source_df["VDO_VIEW_CNT"],
                    "VDO_LIKE_CNT": source_df["VDO_LIKE_CNT"],
                    "VDO_COMMENT_CNT": source_df["VDO_COMMENT_CNT"],
                }),
                when_not_matched().insert({
                    "RECORDED_AT": run_date,
                    "VDO_ID": source_df["VDO_ID"],
                    "VDO_TITLE": source_df["VDO_TITLE"],
                    "PUBLISHED_AT": source_df["PUBLISHED_AT"],
                    "CHANNEL_ID": source_df["CHANNEL_ID"],
                    "CHANNEL_TITLE": source_df["CHANNEL_TITLE"],
                    "VDO_CATEGORY_ID": source_df["VDO_CATEGORY_ID"],
                    "VDO_TAGS": source_df["VDO_TAGS"],
                    "VDO_DEFAUlT_LANGUAGE": source_df["VDO_DEFAUlT_LANGUAGE"],

                    "THUMBNAILS_URL": source_df["THUMBNAILS_URL"],
                    "THUMBNAILS_WIDTH": source_df["THUMBNAILS_WIDTH"],
                    "THUMBNAILS_HEIGHT": source_df["THUMBNAILS_HEIGHT"],

                    "VDO_DURATION": source_df["VDO_DURATION"],
                    "VDO_DEFINITION": source_df["VDO_DEFINITION"],
                    "VDO_CAPTION": source_df["VDO_CAPTION"],

                    "VDO_VIEW_CNT": source_df["VDO_VIEW_CNT"],
                    "VDO_LIKE_CNT": source_df["VDO_LIKE_CNT"],
                    "VDO_COMMENT_CNT": source_df["VDO_COMMENT_CNT"],
                }),
            ],
        )

        print(f"Merged {len(records)} rows into CHANNEL_SNAPSHOT.")
        
    finally:
        session.close()

with DAG(
    dag_id="vdo_s3_to_db",
    start_date=datetime(2026, 10, 1),
    schedule="@hourly",
    catchup=False,
    tags=["youtube", "s3", "ingestion"],
) as dag:
    # ingest_channel_task = PythonOperator(
    #     task_id="ingest_channel_to_snowflake",
    #     python_callable=ingest_channel_to_snowflake,
    # )

    ingest_videos_task = PythonOperator(
        task_id="ingest_video_to_snowflake",
        python_callable=ingest_video_to_snowflake,
    )


