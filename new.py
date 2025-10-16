#!/usr/bin/env python3
"""
APPROUND prototype
Kick → yt-dlp (resumable) → YouTube (private)
Tracks jobs in SQLite and protects against low-disk situations.
"""

import asyncio
import aiosqlite
import json
import os
import sys
import logging
import datetime
import pickle
import subprocess
import shutil

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from kickapi import KickAPI

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
CONFIG_FILE = "config.json"
DB_FILE = "vod_jobs.db"
TOKEN_FILE = "yt_token.pickle"
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("APPROUND")

# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------
def now_iso():
    return datetime.datetime.utcnow().isoformat()


async def init_db():
    """(Re)initialize the SQLite database with correct schema."""
    async with aiosqlite.connect(DB_FILE) as db:
        # create the table fresh if it doesn't exist
        await db.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                streamer TEXT,
                vod_id TEXT,
                title TEXT,
                stream_url TEXT,
                status TEXT,
                file_path TEXT,
                yt_link TEXT,
                error TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        # verify stream_url column exists
        async with db.execute("PRAGMA table_info(jobs)") as cur:
            cols = [row[1] async for row in cur]
        if "stream_url" not in cols:
            # rebuild table if schema is outdated
            log.warning("Outdated DB schema detected; rebuilding...")
            await db.execute("DROP TABLE IF EXISTS jobs")
            await db.execute("""
                CREATE TABLE jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    streamer TEXT,
                    vod_id TEXT,
                    title TEXT,
                    stream_url TEXT,
                    status TEXT,
                    file_path TEXT,
                    yt_link TEXT,
                    error TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            """)
        await db.commit()
        log.info("Database initialized at %s", os.path.abspath(DB_FILE))


async def add_job(streamer, vod_id, title, stream_url):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            INSERT INTO jobs (streamer, vod_id, title, stream_url, status,
                              created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (streamer, vod_id, title, stream_url, "queued", now_iso(), now_iso()),
        )
        await db.commit()


async def job_exists(vod_id):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute(
            "SELECT id FROM jobs WHERE vod_id = ?", (vod_id,)
        ) as cur:
            return await cur.fetchone() is not None


async def update_job(vod_id, **kwargs):
    """Update one job by vod_id."""
    if not kwargs:
        return
    fields = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values())
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            f"UPDATE jobs SET {fields}, updated_at = ? WHERE vod_id = ?",
            (*values, now_iso(), vod_id),
        )
        await db.commit()


# ---------------------------------------------------------------------
# YouTube upload
# ---------------------------------------------------------------------
def yt_authorize():
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "rb") as f:
            creds = pickle.load(f)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "wb") as f:
            pickle.dump(creds, f)
    return creds


def yt_upload(file_path, title, privacy):
    creds = yt_authorize()
    youtube = build("youtube", "v3", credentials=creds)
    body = {
        "snippet": {"title": title, "description": f"Kick VOD backup: {title}"},
        "status": {"privacyStatus": privacy},
    }
    media = MediaFileUpload(file_path, chunksize=-1, resumable=True)
    request = youtube.videos().insert(
        part="snippet,status", body=body, media_body=media
    )
    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            log.info("Upload %.1f%%", status.progress() * 100)
    return f"https://youtu.be/{response['id']}"


# ---------------------------------------------------------------------
# Downloader
# ---------------------------------------------------------------------
async def download_vod(stream_url, output_dir):
    cmd = [
        "yt-dlp",
        "--no-part",
        "--continue",
        "--add-header", "Referer: https://kick.com",
        "--add-header", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "--downloader", "ffmpeg",
        "--downloader-args", "ffmpeg:-headers 'Referer: https://kick.com\\r\\nUser-Agent: Mozilla/5.0\\r\\n'",
        "-o", os.path.join(output_dir, "%(title)s.%(ext)s"),
        stream_url,
    ]
    proc = await asyncio.create_subprocess_exec(*cmd)
    await proc.wait()
    return proc.returncode == 0


# ---------------------------------------------------------------------
# Kick watcher
# ---------------------------------------------------------------------
async def fetch_new_vods(config):
    api = KickAPI()
    for user in config["watchlist"]:
        try:
            channel = api.channel(user)
            for v in channel.videos:
                vod_id = v.id
                title = v.title or f"{user}_{vod_id}"
                stream_url = getattr(v, "stream", None)
                if not stream_url:
                    continue
                if not await job_exists(vod_id):
                    await add_job(user, vod_id, title, stream_url)
                    log.info("New VOD queued: %s – %s", user, title)
        except Exception as exc:
            log.error("Watcher error for %s: %s", user, exc)


# ---------------------------------------------------------------------
# Scheduler loop
# ---------------------------------------------------------------------
async def scheduler(config):
    dl_path = config.get("download_path", "./downloads")
    os.makedirs(dl_path, exist_ok=True)
    interval = config.get("interval_minutes", 10)
    privacy = config.get("youtube", {}).get("privacy", "private")
    sem_dl = asyncio.Semaphore(config.get("max_concurrent_downloads", 1))
    sem_up = asyncio.Semaphore(config.get("max_concurrent_uploads", 1))

    while True:
        log.info("=== Checking Kick for new VODs ===")
        await fetch_new_vods(config)

        # -------------------- Download phase --------------------
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute(
                "SELECT vod_id, streamer, title, stream_url "
                "FROM jobs WHERE status='queued'"
            ) as cur:
                queued = await cur.fetchall()

        for vod_id, streamer, title, stream_url in queued:
            if not stream_url or not stream_url.startswith("http"):
                continue

            # disk-space guard
            free_gb = shutil.disk_usage(dl_path).free / (1024 ** 3)
            if free_gb < 5:
                log.warning("Low disk space (<5 GB); pausing downloads.")
                break

            async with sem_dl:
                log.info("Downloading %s – %s", streamer, title)
                ok = await download_vod(stream_url, dl_path)
                if ok:
                    mp4s = [f for f in os.listdir(dl_path) if f.endswith(".mp4")]
                    if mp4s:
                        latest = max(
                            mp4s,
                            key=lambda f: os.path.getmtime(os.path.join(dl_path, f)),
                        )
                        file_path = os.path.join(dl_path, latest)
                        await update_job(
                            vod_id, status="downloaded", file_path=file_path
                        )
                        log.info("Downloaded: %s", file_path)
                else:
                    log.warning(f"Download failed for {title}; will retry later.")
                    await update_job(vod_id, status="queued", error="temporary failure")


        # -------------------- Upload phase --------------------
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute(
                "SELECT vod_id, file_path, title FROM jobs WHERE status='downloaded'"
            ) as cur:
                to_upload = await cur.fetchall()

        for vod_id, file_path, title in to_upload:
            if not os.path.isfile(file_path):
                continue
            async with sem_up:
                try:
                    log.info("Uploading %s to YouTube", title)
                    link = yt_upload(file_path, title, privacy)
                    await update_job(vod_id, status="done", yt_link=link)
                    log.info("Uploaded → %s", link)
                except Exception as exc:
                    log.error("Upload failed for %s: %s", title, exc)
                    await update_job(
                        vod_id, status="error", error=str(exc)
                    )

        log.info("Cycle complete — sleeping %d min\n", interval)
        await asyncio.sleep(interval * 60)


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------
async def main():
    if not os.path.exists(CONFIG_FILE):
        log.error("Missing config.json")
        sys.exit(1)
    with open(CONFIG_FILE, encoding="utf-8") as f:
        config = json.load(f)
    await init_db()
    await scheduler(config)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user.")
