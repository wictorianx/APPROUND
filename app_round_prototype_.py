#!/usr/bin/env python3
# ==========================================================
# APPROUND Prototype — Kick → yt-dlp → YouTube Auto Uploader
# ==========================================================
import asyncio, aiosqlite, json, os, subprocess, datetime, time, sys, logging
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
from kickapi import KickAPI

# ----------------------------------------------------------
# Config / Globals
# ----------------------------------------------------------
CONFIG_FILE = "config.json"
DB_FILE = "vod_jobs.db"
TOKEN_FILE = "yt_token.pickle"
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

# ----------------------------------------------------------
# Logging setup
# ----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("APPROUND")

# ----------------------------------------------------------
# Utilities
# ----------------------------------------------------------
def now_iso():
    return datetime.datetime.utcnow().isoformat()

async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            streamer TEXT,
            vod_id TEXT,
            title TEXT,
            status TEXT,
            file_path TEXT,
            yt_link TEXT,
            error TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """)
        await db.commit()

async def add_job(streamer, vod_id, title):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT INTO jobs (streamer, vod_id, title, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (streamer, vod_id, title, "queued", now_iso(), now_iso())
        )
        await db.commit()

async def job_exists(vod_id):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT id FROM jobs WHERE vod_id = ?", (vod_id,)) as cur:
            return await cur.fetchone() is not None

async def update_job(vod_id, **kwargs):
    fields = ", ".join([f"{k} = ?" for k in kwargs])
    values = list(kwargs.values()) + [vod_id]
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(f"UPDATE jobs SET {fields}, updated_at = ? WHERE vod_id = ?",
                         (*kwargs.values(), now_iso(), vod_id))
        await db.commit()

# ----------------------------------------------------------
# YouTube uploader
# ----------------------------------------------------------
def yt_authorize():
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "rb") as f:
            creds = pickle.load(f)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "wb") as f:
            pickle.dump(creds, f)
    return creds

def yt_upload(file_path, title, privacy):
    creds = yt_authorize()
    youtube = build("youtube", "v3", credentials=creds)
    body = {
        "snippet": {"title": title, "description": f"Auto-uploaded from Kick VOD: {title}"},
        "status": {"privacyStatus": privacy}
    }
    media = MediaFileUpload(file_path, chunksize=-1, resumable=True)
    req = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    response = None
    while response is None:
        status, response = req.next_chunk()
        if status:
            print(f"Uploading {int(status.progress() * 100)}%...")
    return f"https://youtu.be/{response['id']}"

# ----------------------------------------------------------
# Downloader
# ----------------------------------------------------------
async def download_vod(vod_url, output_dir):
    cmd = [
        "yt-dlp",
        "--no-overwrites",
        "--continue",
        "-o", os.path.join(output_dir, "%(title)s.%(ext)s"),
        vod_url
    ]
    proc = await asyncio.create_subprocess_exec(*cmd)
    await proc.wait()
    return proc.returncode == 0

# ----------------------------------------------------------
# Kick watcher
# ----------------------------------------------------------
async def fetch_new_vods(config):
    api = KickAPI()
    new_jobs = []
    for user in config["watchlist"]:
        try:
            channel = api.channel(user)
            vods = channel.videos
            for v in vods:
                vod_id = v.id
                title = v.title or f"{user}_{vod_id}"
                stream_url = getattr(v, "stream", None)
                if not stream_url:
                    continue  # skip if API didn’t return stream field yet
                if not await job_exists(vod_id):
                    await add_job(user, vod_id, title)
                    # also store stream url for later
                    async with aiosqlite.connect(DB_FILE) as db:
                        await db.execute("UPDATE jobs SET file_path=? WHERE vod_id=?", (stream_url, vod_id))
                        await db.commit()
                    new_jobs.append((user, vod_id, title))
                    log.info(f"New VOD found: {user} - {title}")

    return new_jobs

# ----------------------------------------------------------
# Main scheduler loop
# ----------------------------------------------------------
async def scheduler(config):
    download_path = config.get("download_path", "./downloads")
    os.makedirs(download_path, exist_ok=True)
    interval = config.get("interval_minutes", 10)
    privacy = config["youtube"].get("privacy", "private")
    sem_dl = asyncio.Semaphore(config.get("max_concurrent_downloads", 1))
    sem_up = asyncio.Semaphore(config.get("max_concurrent_uploads", 1))

    while True:
        log.info("=== Checking for new VODs ===")
        await fetch_new_vods(config)

        # Get queued jobs
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute("SELECT vod_id, streamer, title, status FROM jobs WHERE status='queued'") as cur:
                queued = await cur.fetchall()

        for vod_id, streamer, title, status in queued:
            async with sem_dl:
                log.info(f"Downloading {title} from {streamer}")
                vod_url = f"https://kick.com/{streamer}/videos/{vod_id}"
                ok = await download_vod(vod_url, download_path)
                if ok:
                    # locate downloaded file
                    mp4s = [f for f in os.listdir(download_path) if f.endswith(".mp4")]
                    if mp4s:
                        file_path = os.path.join(download_path, mp4s[-1])
                        await update_job(vod_id, status="downloaded", file_path=file_path)
                        log.info(f"Downloaded: {file_path}")
                else:
                    await update_job(vod_id, status="error", error="Download failed")
                    continue

        # Upload downloaded ones
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute("SELECT vod_id, file_path, title FROM jobs WHERE status='downloaded'") as cur:
                to_upload = await cur.fetchall()

        for vod_id, file_path, title in to_upload:
            async with sem_up:
                log.info(f"Uploading {title} to YouTube...")
                try:
                    link = yt_upload(file_path, title, privacy)
                    await update_job(vod_id, status="done", yt_link=link)
                    log.info(f"Uploaded: {link}")
                except Exception as e:
                    log.error(f"Upload error for {title}: {e}")
                    await update_job(vod_id, status="error", error=str(e))

        log.info(f"Cycle complete. Sleeping {interval} minutes...\n")
        await asyncio.sleep(interval * 60)

# ----------------------------------------------------------
# Entry point
# ----------------------------------------------------------
async def main():
    if not os.path.exists(CONFIG_FILE):
        log.error("Missing config.json!")
        sys.exit(1)
    with open(CONFIG_FILE) as f:
        config = json.load(f)
    await init_db()
    await scheduler(config)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
