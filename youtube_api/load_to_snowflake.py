for video in response["items"]:

            videos_details.append({
                "video_id": video["id"],
                "title": video["snippet"]["title"],
                "description": video["snippet"]["description"],
                "published_at": video["snippet"]["publishedAt"],
                "duration": video["contentDetails"]["duration"],
                "view_count": video["statistics"].get("viewCount"),
                "like_count": video["statistics"].get("likeCount"),
                "comment_count": video["statistics"].get("commentCount"),
            })


for item in response["items"]:

            videos.append({
                "channel_id": channel_id,
                "playlist_item_id": item["id"],
                "playlist_title": item["snippet"]["title"],
                "video_id": item["contentDetails"]["videoId"],
                "published_at": item["snippet"]["publishedAt"],
            })
# ---------------------------------------------------------------------------
# Opens a new Snowflake connection
# Call this once and reuse the returned conn across multiple fetches rather
# ---------------------------------------------------------------------------
def get_connection():
    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema=os.environ['SNOWFLAKE_SCHEMA'],
        # role=os.environ['SNOWFLAKE_ROLE'],
        authenticator='externalbrowser',
    )

    session_info = conn.cursor().execute(
        "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
    ).fetchone()
    print(session_info)
    

    return conn


def load_raw_data(conn, df):
    try:
        # Polars → Pandas
        pdf = df.to_pandas()

        # Clear existing data
        conn.cursor().execute("TRUNCATE TABLE MONTHLY_TEMP_TABLE")

        # Load DataFrame directly into Snowflake
        success, nchunks, nrows, output = write_pandas(
            conn,
            pdf,
            "VIDEO_",
            auto_create_table=False,
            overwrite=False
        )

        print(f"Loaded {nrows} rows into MONTHLY_TEMP_TABLE")
        print(output)

        if not success:
            raise RuntimeError("Failed to load data into Snowflake")

    finally:
        conn.close()
