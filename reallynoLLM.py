from kickapi import KickAPI
import os, shutil, subprocess
import google.auth.transport.requests
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

kick_api = KickAPI()
dl_path="./downloads/"

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

def get_authenticated_service(credentials_json):
    flow = InstalledAppFlow.from_client_secrets_file(credentials_json, SCOPES)
    creds = flow.run_console()  # opens terminal prompt for auth
    return build("youtube", "v3", credentials=creds)

def upload_to_youtube(file_path, title, description="", tags=None, privacy="private", credentials_json="credentials.json"):
    """
    Upload a video to YouTube.
    
    Args:
        file_path (str): Path to the video file.
        title (str): Video title.
        description (str): Video description.
        tags (list[str]): List of tags.
        privacy (str): "public", "unlisted", or "private".
        credentials_json (str): Path to your Google API credentials.json
    """
    youtube = get_authenticated_service(credentials_json)

    media = MediaFileUpload(file_path, chunksize=-1, resumable=True)
    request = youtube.videos().insert(
        part="snippet,status",
        body={
            "snippet": {
                "title": title,
                "description": description,
                "tags": tags or [],
            },
            "status": {
                "privacyStatus": privacy
            }
        },
        media_body=media
    )

    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            print(f"Upload progress: {int(status.progress() * 100)}%")

    print(f"Upload complete! Video ID: {response['id']}")
    return response["id"]

def download_ffmpeg(stream_id, dl_path=dl_path):
    video = kick_api.video(stream_id)
    stream = video.stream
    free_gb = shutil.disk_usage(dl_path).free / (1024**3)
    duration_hours = video.duration / 1000 / 3600
    est_size = duration_hours
    title = video.title
    print(free_gb , est_size)
    if free_gb > est_size:
        safe = "".join(c for c in title if c.isalnum() or c in (" ", "_", "-", ".")).strip() or "vod"
        out_mp4 = os.path.join(dl_path, f"{safe}.mp4")

        # just copy streams, no re-encoding
        cmd = [
            "ffmpeg",
            "-y",
            "-i", stream,
            "-c", "copy",  # copy audio & video without transcoding
            out_mp4
        ]

        subprocess.run(cmd, check=True)
    return True

download_ffmpeg("b746cb54-1dba-46de-8e15-0b4325699855")
