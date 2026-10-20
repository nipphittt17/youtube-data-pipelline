
import json
import logging
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from googleapiclient.discovery import build

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from youtube_assets import CHANNEL_SNAPSHOT_ASSET, VIDEO_SNAPSHOT_ASSET

logger = logging.getLogger(__name__)

def channel_setup():
    return [
            ("dice", os.environ["DICE_CHANNEL_ID"]),
            ("dice_blue", os.environ["DICE_BLUE_CHANNEL_ID"]),
            ("bus", os.environ["BUS_CHANNEL_ID"]),
            ("tada", os.environ["TADA_CHANNEL_ID"]),
    ]

def s3_setup():
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

def youtube_api_setup():
    youtube = build(
        "youtube",
        "v3",
        developerKey=os.environ["API_KEY"],
        cache_discovery=False,
    )
    
    channels = channel_setup()
    return youtube, channels

# --------------------------------------------------
# Fetch all channel infomration for channel IDs
# --------------------------------------------------
def get_channel(youtube, channel_id):
    response = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id,
    ).execute()
    
    channel = response.get("items", [])
    return channel

# --------------------------------------------------
# Fetch all playlist items from a channel
# --------------------------------------------------

def get_videos(youtube, channel_id):
    response = youtube.channels().list(
        part="contentDetails",
        id=channel_id,
    ).execute()

    items = response.get("items", [])

    if not items:
        raise ValueError(f"Channel not found: {channel_id}")

    playlist_id = (
        items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
    )

    videos = []
    next_page_token = None

    while True:
        response = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            pageToken=next_page_token,
            maxResults=50,
        ).execute()

        videos.extend(response.get("items", []))

        next_page_token = response.get("nextPageToken")

        if not next_page_token:
            break

    return videos

# --------------------------------------------------
# Fetch detailed information for video IDs
# --------------------------------------------------

def get_video_details(youtube, videos):
    video_ids = [
        video["contentDetails"]["videoId"]
        for video in videos
        if video.get("contentDetails", {}).get("videoId")
    ]

    videos_details = []

    # YouTube API supports up to 50 video IDs per request.
    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i + 50]

        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(batch_ids),
        ).execute()

        videos_details.extend(response.get("items", []))

    return videos_details

# --------------------------------------------------
# Task: Fetch channel items and upload to S3
# --------------------------------------------------
def load_channel_s3(**context):

    bucket, s3 = s3_setup()

    youtube, channels = youtube_api_setup()

    logger.info(
        "Update channel information for %d channels.",
        len(channels),
    )

    ds = context["ds_nodash"]

    for channel, channel_id in channels:
        logger.info(
            "Fetching channel info for channel '%s'.",
            channel,
        )
        channel_items = get_channel(youtube, channel_id)

        key = (
            f"channel-snapshot/"
            f"cs-{ds}-{channel_id}.json"
        )

        try:
            logger.info(
                "Uploading %d channel info to s3://%s/%s",
                len(channel_items),
                bucket,
                key,
            )

            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(
                    channel_items,
                    ensure_ascii=False,
                ).encode("utf-8"),
                ContentType="application/json",
            )

        except ClientError:
            logger.exception(
                "S3 upload failed for channel '%s'.",
                channel,
            )
            raise


        logger.info(
            "Successfully uploaded channel info for '%s'.",
            channel,
        )

    logger.info("Channel ingestion completed successfully.")

# --------------------------------------------------
# Task: Fetch playlist items and upload to S3 + Xcom
# --------------------------------------------------
def load_video_s3(**context):

    bucket, s3 = s3_setup()
    youtube, channels = youtube_api_setup()

    logger.info(
        "Starting playlist ingestion for %d channels.",
        len(channels),
    )

    s3_keys = {}
    ds = context["ds_nodash"]

    for channel, channel_id in channels:

        videos = get_videos(youtube, channel_id)

        key = (
            f"playlist-snapshot/"
            f"ps-{ds}-{channel_id}.json"
        )

        try:
            logger.info(
                "Uploading %d playlist items to s3://%s/%s",
                len(videos),
                bucket,
                key,
            )

            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(
                    videos,
                    ensure_ascii=False,
                ).encode("utf-8"),
                ContentType="application/json",
            )

        except ClientError:
            logger.exception(
                "Playlist upload failed for channel '%s'.",
                channel,
            )
            raise

        s3_keys[channel_id] = key

        logger.info(
            "Successfully uploaded playlist items for '%s'.",
            channel,
        )

    logger.info("Playlist ingestion completed successfully.")

    # Small payload passed to the next Airflow task via XCom.
    return s3_keys

# --------------------------------------------------
# Task: Read playlist items and fetch video details
# --------------------------------------------------
def load_video_detail_s3(**context):

    bucket, s3 = s3_setup()
    youtube, channels = youtube_api_setup()

    # Retrieve the S3 keys returned by Task 2.
    s3_keys = context["ti"].xcom_pull(
        task_ids="load_video_s3",
    )

    if not s3_keys:
        raise ValueError(
            "No S3 object keys found from load_video_s3."
        )

    ds = context["ds_nodash"]

    logger.info("Starting video details ingestion.")

    for channel, channel_id in channels:
        source_key = s3_keys.get(channel_id)

        if not source_key:
            raise ValueError(
                f"No playlist S3 key found for channel '{channel}'."
            )

        logger.info(
            "Reading playlist items from s3://%s/%s",
            bucket,
            source_key,
        )

        try:
            # Download playlist JSON from S3.
            response = s3.get_object(
                Bucket=bucket,
                Key=source_key,
            )

            videos = json.loads(
                response["Body"].read().decode("utf-8")
            )

        except ClientError:
            logger.exception(
                "Failed to read playlist data for '%s'.",
                channel,
            )
            raise

        logger.info(
            "Retrieved %d playlist items for '%s'.",
            len(videos),
            channel,
        )

        # Fetch full details using the video IDs.
        details = get_video_details(
            youtube,
            videos,
        )

        key = (
            f"video-snapshot/"
            f"vs-{ds}-{channel_id}.json"
        )

        try:
            logger.info(
                "Uploading %d video details to s3://%s/%s",
                len(details),
                bucket,
                key,
            )

            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(
                    details,
                    ensure_ascii=False,
                ).encode("utf-8"),
                ContentType="application/json",
            )

        except ClientError:
            logger.exception(
                "Video details upload failed for '%s'.",
                channel,
            )
            raise

        logger.info(
            "Successfully uploaded video details for '%s'.",
            channel,
        )

    logger.info("Video details ingestion completed successfully.")


def extract_folder_s3(s3_folder, s3_date):
    bucket, s3 = s3_setup()
    paginator = s3.get_paginator("list_objects_v2")

    json_data = []
    
    if s3_folder == 'channel-snapshot/':
        s3_prefix = 'cs-'
    elif s3_folder == 'video-snapshot/':
        s3_prefix = 'vs-'

    target_prefix = f"{s3_folder}{s3_prefix}{s3_date}"

    for page in paginator.paginate(
            Bucket=bucket,
            Prefix=target_prefix,
        ):

        for obj in page.get("Contents", []):
            s3_key = obj["Key"]
            filename = os.path.basename(s3_key)

            if not filename.startswith(f"{s3_prefix}{s3_date}"):
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
    if isinstance(data, list):
        return data
    
    if isinstance(data, dict):
        items = data.get("items")
        return items if isinstance(items, list) else [data]

    print(f"Skipping unexpected JSON structure: {prefix}")
    return []

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

# --------------------------------------------------
# Transform channel data from JSON to Dataframe
# --------------------------------------------------
def transform_channel_data(run_date, s3_date):
    import polars as pl

    channels = []
    prefix = "channel-snapshot/"

    for data in extract_folder_s3(prefix, s3_date):

        for channel in extract_items(data, prefix):

            if not isinstance(channel, dict):
                continue

            stats = channel.get("statistics", {})
            snippet = channel.get("snippet", {})

            channels.append({
                "RECORDED_AT": run_date,
                "CHANNEL_ID": channel.get("id"),
                "USERNAME": snippet.get("customUrl"),
                "CHANNEL_TITLE": snippet.get("title"),
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

# --------------------------------------------------
# Transform video data from JSON to Dataframe
# --------------------------------------------------
def transform_video_data(run_date, s3_date):
    import polars as pl

    videos = []
    prefix = "video-snapshot/"

    for data in extract_folder_s3(prefix, s3_date):
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
                "VDO_DESCRIPTION" : snippet.get("description"),

                "VDO_CATEGORY_ID": snippet.get("categoryId"),
                "VDO_TAGS": snippet.get("tags"),
                "VDO_DEFAUlT_LANGUAGE": snippet.get("defaultAudioLanguage"),
                 "CHANNEL_ID": snippet.get("channelId"),

                "THUMBNAILS_URL": default_thumbnail.get("url"),
                # "THUMBNAILS_WIDTH": default_thumbnail.get("width"),
                # "THUMBNAILS_HEIGHT": default_thumbnail.get("height"),

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

# --------------------------------------------------
# Task: Load channel data into Snowflake [merge into]
# --------------------------------------------------
def load_channel_snowflake(**context):
    from snowflake.snowpark.functions import when_matched, when_not_matched

    s3_date = context["ds_nodash"]
    run_date = datetime.strptime(s3_date, "%Y%m%d")
    channel_df = transform_channel_data(run_date, s3_date)

    if channel_df.is_empty():
        print("No channel data found. Skipping ingestion.")
        return

    session = get_snowpark_session()

    try:
        records = channel_df.to_dicts()
        target_table = session.table("STG_CHANNEL_DAILY_SNAPSHOT")
        source_df = session.create_dataframe(records)

        join_condition = (
            (target_table["CHANNEL_ID"] == source_df["CHANNEL_ID"])
            & (target_table["RECORDED_AT"] == source_df["RECORDED_AT"])
        )

        target_table.merge(
            source=source_df,
            join_expr=join_condition,
            clauses=[
                when_matched().update({
                    "USERNAME": source_df["USERNAME"],
                    "CHANNEL_TITLE": source_df["CHANNEL_TITLE"],
                    "PUBLISHED_AT": source_df["PUBLISHED_AT"],
                    "TOTAL_VIEW": source_df["TOTAL_VIEW"],
                    "TOTAL_SUBSCRIBER": source_df["TOTAL_SUBSCRIBER"],
                    "TOTAL_VIDEO": source_df["TOTAL_VIDEO"],
                }),
                when_not_matched().insert({
                    "RECORDED_AT": source_df["RECORDED_AT"],
                    "CHANNEL_ID": source_df["CHANNEL_ID"],
                    "USERNAME": source_df["USERNAME"],
                    "CHANNEL_TITLE": source_df["CHANNEL_TITLE"],
                    "PUBLISHED_AT": source_df["PUBLISHED_AT"],
                    "TOTAL_VIEW": source_df["TOTAL_VIEW"],
                    "TOTAL_SUBSCRIBER": source_df["TOTAL_SUBSCRIBER"],
                    "TOTAL_VIDEO": source_df["TOTAL_VIDEO"],
                }),
            ],
        )

        print(f"Merged {len(records)} rows into STG_CHANNEL_DAILY_SNAPSHOT.")

    finally:
        session.close()

# --------------------------------------------------
# Task: Load video data into Snowflake [merge into]
# --------------------------------------------------
def load_video_snowflake(**context):
    from snowflake.snowpark.functions import when_matched, when_not_matched

    s3_date = context["ds_nodash"]
    run_date = datetime.strptime(s3_date, "%Y%m%d")
    video_df = transform_video_data(run_date, s3_date)

    if video_df.is_empty():
        print("No video data found. Skipping ingestion.")
        return

    session = get_snowpark_session()

    try:
        records = video_df.to_dicts()
        target_table = session.table("STG_VIDEO_DAILY_SNAPSHOT")
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
                    "VDO_TITLE": source_df["VDO_TITLE"],
                    "PUBLISHED_AT": source_df["PUBLISHED_AT"],

                    "VDO_DESCRIPTION": source_df["VDO_DESCRIPTION"],
                    "VDO_CATEGORY_ID": source_df["VDO_CATEGORY_ID"],
                    "VDO_TAGS": source_df["VDO_TAGS"],
                    "VDO_DEFAUlT_LANGUAGE": source_df["VDO_DEFAUlT_LANGUAGE"],

                    "CHANNEL_ID": source_df["CHANNEL_ID"],
                    "THUMBNAILS_URL": source_df["THUMBNAILS_URL"],

                    "VDO_DURATION": source_df["VDO_DURATION"],
                    "VDO_DEFINITION": source_df["VDO_DEFINITION"],
                    "VDO_CAPTION": source_df["VDO_CAPTION"],

                    "VDO_VIEW_CNT": source_df["VDO_VIEW_CNT"],
                    "VDO_LIKE_CNT": source_df["VDO_LIKE_CNT"],
                    "VDO_COMMENT_CNT": source_df["VDO_COMMENT_CNT"],
                }),
                when_not_matched().insert({
                    "RECORDED_AT": source_df["RECORDED_AT"],
                    "VDO_ID": source_df["VDO_ID"],
                    "VDO_TITLE": source_df["VDO_TITLE"],
                    "PUBLISHED_AT": source_df["PUBLISHED_AT"],

                    "VDO_DESCRIPTION": source_df["VDO_DESCRIPTION"],
                    "VDO_CATEGORY_ID": source_df["VDO_CATEGORY_ID"],
                    "VDO_TAGS": source_df["VDO_TAGS"],
                    "VDO_DEFAUlT_LANGUAGE": source_df["VDO_DEFAUlT_LANGUAGE"],

                    "CHANNEL_ID": source_df["CHANNEL_ID"],
                    "THUMBNAILS_URL": source_df["THUMBNAILS_URL"],

                    "VDO_DURATION": source_df["VDO_DURATION"],
                    "VDO_DEFINITION": source_df["VDO_DEFINITION"],
                    "VDO_CAPTION": source_df["VDO_CAPTION"],

                    "VDO_VIEW_CNT": source_df["VDO_VIEW_CNT"],
                    "VDO_LIKE_CNT": source_df["VDO_LIKE_CNT"],
                    "VDO_COMMENT_CNT": source_df["VDO_COMMENT_CNT"],
                }),
            ],
        )

        print(f"Merged {len(records)} rows into STG_VIDEO_DAILY_SNAPSHOT.")

    finally:
        session.close()

with DAG(
    dag_id="youtube_daily_ingestion",
    start_date=datetime(2026, 10, 15),
    schedule="@daily",
    catchup=False,
    tags=["youtube", "s3", "ingestion"],
) as dag:

    load_channel_s3_task = PythonOperator(
        task_id="load_channel_s3",
        python_callable=load_channel_s3,
    )

    load_video_s3_task = PythonOperator(
        task_id="load_video_s3",
        python_callable=load_video_s3,
    )

    load_video_detail_s3_task = PythonOperator(
        task_id="load_video_detail_s3",
        python_callable=load_video_detail_s3,
    )

    load_channel_snowflake_task = PythonOperator(
        task_id="load_channel_snowflake",
        python_callable=load_channel_snowflake,
        outlets=[CHANNEL_SNAPSHOT_ASSET],
    )

    load_video_snowflake_task = PythonOperator(
        task_id="load_video_snowflake",
        python_callable=load_video_snowflake,
        outlets=[VIDEO_SNAPSHOT_ASSET],
    )

    load_channel_s3_task >> load_channel_snowflake_task
    load_video_s3_task >> load_video_detail_s3_task >> load_video_snowflake_task
