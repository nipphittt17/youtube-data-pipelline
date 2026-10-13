import os
from dotenv import load_dotenv

import polars as pl
from googleapiclient.discovery import build

import json

load_dotenv(override=True)

def build_youtube():
    youtube = build(
        "youtube",
        "v3",
        developerKey=os.environ["API_KEY"]
    )
    return youtube

def get_videos(youtube, channel_id):
    
    # Get the channel's uploads playlist
    response = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()

    channel = response["items"][0]

    uploads_playlist_id = (
        channel["contentDetails"]
        ["relatedPlaylists"]
        ["uploads"]
    )

    # Get videos from the uploads playlist
    videos = []

    next_page_token = None

    while True:

        response = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=uploads_playlist_id,
            pageToken=next_page_token
        ).execute()

        videos.extend(response.get("items", []))

        next_page_token = response.get("nextPageToken")

        if not next_page_token:
            break

    return videos


# def get_video_details(youtube, video_ids):
#     videos_details = []

#     for i in range(0, len(video_ids), 50):
#         batch = video_ids[i:i + 50]

#         response = youtube.videos().list(
#             part="snippet,contentDetails,statistics",
#             id=",".join(batch)
#         ).execute()

#         videos_details.extend(response.get("items", []))

#     return videos_details


def main():
    youtube = build_youtube()

    for channel_id in [os.environ['DICE_CHANNEL_ID'],os.environ['TADA_CHANNEL_ID']]:
        videos = get_videos(youtube, channel_id)
    with open("data.json", "w", encoding="utf-8") as file:
        json.dump(videos, file)


if __name__ == "__main__":
    main()




    