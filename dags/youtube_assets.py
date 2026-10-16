from airflow.sdk import Asset
# youtube_transformation is scheduled on these, so dbt runs once both are refreshed.
CHANNEL_SNAPSHOT_ASSET = Asset(name="stg_channel_daily_snapshot")
VIDEO_SNAPSHOT_ASSET = Asset(name="stg_video_daily_snapshot")
