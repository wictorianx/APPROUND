from datetime import datetime
from kickapi import KickAPI


def get_video_stream_url(video_url: str) -> str | None:
    try:
        parts = video_url.split("/")
        channel_name = parts[3]
        video_slug = parts[5]

        kick_api = KickAPI()
        channel = kick_api.channel(channel_name)

        for video in channel.videos:
            if video.uuid == video_slug:
                thumbnail_url = video.thumbnail["src"]
                start_time = datetime.strptime(video.start_time, "%Y-%m-%d %H:%M:%S")
                path_parts = thumbnail_url.split("/")
                channel_id, video_id = path_parts[4], path_parts[5]

                stream_url = (
                    f"https://stream.kick.com/ivs/v1/196233775518/"
                    f"{channel_id}/{start_time.year}/{start_time.month}/"
                    f"{start_time.day}/{start_time.hour}/{start_time.minute}/"
                    f"{video_id}/media/hls/master.m3u8"
                )
                return stream_url
        return None
    except Exception as e:
        return None

