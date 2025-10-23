from kickapi import KickAPI
import shutil
import os
from ffmpeg import FFmpeg


kick_api = KickAPI()
dl_path="./downloads/"






def download_ffmpeg(stream_id):
    video = kick_api.video(stream_id)
    stream = video.stream
    free_gb = shutil.disk_usage(dl_path).free / (1024**3)
    duration = video.duration/1000/3600
    est_size = duration
    title=video.title
    #print(free_gb, est_size)
    if free_gb>est_size:
        safe = "".join(c for c in title if c.isalnum() or c in (" ", "_", "-", ".")).strip() or "vod"
        out_mp4 = os.path.join(dl_path, f"{safe}.mp4")
        ffmpeg = (
            FFmpeg()
            .option("y")
            .input(stream)
            .output(
                out_mp4,
                {"codec:v": "libx264"},
                vf="scale=1280:-1",
                preset="veryslow",
                crf=24,
            )
        )
        ffmpeg.execute()
download_vod("98d3d561-9a4b-499b-aac2-d384efe9bda5")

