import logging
import boto3
import os
import json
import datetime as dt

from botocore.exceptions import ClientError
from get_videos import build_youtube, get_videos


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)


def main():
    s3 = boto3.client("s3", region_name="ap-southeast-2")
    youtube = build_youtube()

    bucket = "dice-snowflake-bucket-830523816673-ap-southeast-2-an"
    today = dt.date.today().strftime("%Y%m%d")
    current_time = dt.date.today().strftime("%H:%M:%S")

    channels = [
        ("dice", os.environ["DICE_CHANNEL_ID"]),
        ("sonray", os.environ["TADA_CHANNEL_ID"])
    ]

    logger.info("Starting YouTube data ingestion for %d channels.", len(channels))

    for channel, channel_id in channels:
        key = f"youtube-videos-{channel}/{today}-{current_time}-{channel_id}.json"

        try:
            logger.info("Fetching videos for channel '%s'.", channel)

            videos = get_videos(youtube, channel_id)

            logger.info(
                "Retrieved %d videos for channel '%s'.",
                len(videos),
                channel
            )

            logger.info("Uploading data to s3://%s/%s", bucket, key)

            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(videos, ensure_ascii=False),
                ContentType="application/json"
            )

            logger.info(
                "Successfully uploaded data for channel '%s'.",
                channel
            )

        except ClientError:
            logger.exception(
                "S3 upload failed for channel '%s'.",
                channel
            )
            raise

    logger.info("YouTube data ingestion completed successfully.")


if __name__ == "__main__":
    main()