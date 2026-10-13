from googleapiclient.discovery import build

API_KEY = "AIzaSyBwFlrMhYOf6cNSqAO5fB40UPVX9q8Iq9k"

youtube = build(
    "youtube",
    "v3",
    developerKey=API_KEY
)

response = youtube.channels().list(
    part="id,snippet,contentDetails",
    forHandle="@TADALABELS"
).execute()

channel = response["items"][0]

channel_id = channel["id"]

print(channel_id)

