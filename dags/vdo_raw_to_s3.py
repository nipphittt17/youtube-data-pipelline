
import json
import logging
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from googleapiclient.discovery import build

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

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


def channel_setup():
    return [
            ("dice", os.environ["DICE_CHANNEL_ID"]),
            ("sonray", os.environ["TADA_CHANNEL_ID"]),
    ]


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
# Task 1: Fetch channel items and upload to S3
# --------------------------------------------------
def ingest_channels():
    
    bucket, s3 = s3_setup()

    youtube, channels = youtube_api_setup()

    logger.info(
        "Update channel information for %d channels.",
        len(channels),
    )

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    for channel, channel_id in channels:
        logger.info(
            "Fetching channel info for channel '%s'.",
            channel,
        )
        channels = get_channel(youtube, channel_id)
    
        key = (
            f"youtube-{channel}/"
            f"{timestamp}-{channel_id}.json"
        )

        try:
            logger.info(
                "Uploading %d channel info to s3://%s/%s",
                len(channels),
                bucket,
                key,
            )

            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(
                    channels,
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
# Task2: Fetch playlist items and upload to S3 + Xcom
# --------------------------------------------------

def ingest_videos():

    bucket, s3 = s3_setup()
    youtube, channels = youtube_api_setup()

    logger.info(
        "Starting playlist ingestion for %d channels.",
        len(channels),
    )

    s3_keys = {}
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    for channel, channel_id in channels:

        videos = get_videos(youtube, channel_id)

        # Store each channel's playlist items separately.
        key = (
            f"youtube-videos-{channel}/"
            f"{timestamp}-{channel_id}.json"
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
                "Platlist upload failed for channel '%s'.",
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
# Task3: Read playlist items and fetch video details
# --------------------------------------------------

def ingest_video_details(**context):
    
    bucket, s3 = s3_setup()
    youtube, channels = youtube_api_setup()

    # Retrieve the S3 keys returned by Task 2.
    s3_keys = context["ti"].xcom_pull(
        task_ids="ingest_videos",
    )

    if not s3_keys:
        raise ValueError(
            "No S3 object keys found from ingest_videos."
        )

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

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

        # Save details in a separate S3 prefix.
        output_key = (
            f"youtube-video-details-{channel}/"
            f"{timestamp}-{channel_id}.json"
        )

        try:
            logger.info(
                "Uploading %d video details to s3://%s/%s",
                len(details),
                bucket,
                output_key,
            )

            s3.put_object(
                Bucket=bucket,
                Key=output_key,
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

with DAG(
    dag_id="vdo_raw_to_s3",
    start_date=datetime(2026, 10, 1),
    schedule="@hourly",
    catchup=False,
    tags=["youtube", "s3", "ingestion"],
) as dag:

    ingest_channels = PythonOperator(
            task_id="ingest_channels",
            python_callable=ingest_channels,
    )

    ingest_videos = PythonOperator(
        task_id="ingest_videos",
        python_callable=ingest_videos,
    )

    ingest_video_details = PythonOperator(
        task_id="ingest_video_details",
        python_callable=ingest_video_details,
    )

    ingest_channels >> ingest_videos >> ingest_video_details
