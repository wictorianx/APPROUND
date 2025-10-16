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
log.addHandler(logging.StreamHandler(sys.stdout))


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
async def download_vod(
    stream_url: str,
    output_dir: str,
    title: str,
    *,
    min_free_gb: float = 2.0,
    max_retries: int = 4,
    retry_backoff: float = 5.0,
    ffmpeg_bin: str = "ffmpeg"
) -> tuple[bool, str | None]:
    """
    Robust ffmpeg-based downloader for Kick HLS (.m3u8) with pseudo-resume.

    Behavior:
    - Writes to "<title>.mp4.part" while downloading, moves to "<title>.mp4" on success.
    - Skips download if final MP4 already exists.
    - Checks free disk space before starting (min_free_gb).
    - Retries transient failures with exponential backoff (max_retries).
    - Passes Referer and User-Agent headers which Kick's CDN often requires.
    - Returns (True, path_to_file) on success, (False, None) on failure.

    Notes:
    - This is *pseudo-resume*: ffmpeg won't reconstruct partially-complete HLS segments
      if the stream URL expired. We keep the .part file to avoid accidental overwrites
      and to let you inspect partial state. Failed jobs should be retried later where
      the scheduler can refresh the stream_url and re-run this function.
    """

    safe_title = "".join(c for c in title if c.isalnum() or c in (" ", "_", "-", ".")).strip()
    if not safe_title:
        safe_title = "vod"
    out_mp4 = os.path.join(output_dir, f"{safe_title}.mp4")
    temp_path = out_mp4 + ".part"

    # Quick skip if already downloaded
    if os.path.exists(out_mp4) and os.path.getsize(out_mp4) > 1024:
        log.info("Download skipped (already exists): %s", out_mp4)
        return True, out_mp4

    # ensure output dir
    os.makedirs(output_dir, exist_ok=True)

    # Disk-space guard
    try:
        free_gb = shutil.disk_usage(output_dir).free / (1024 ** 3)
        if free_gb < min_free_gb:
            log.warning("Not enough free disk space (%.2f GB) to start download: need >= %.2f GB",
                        free_gb, min_free_gb)
            return False, None
    except Exception as e:
        log.warning("Unable to check disk space: %s (continuing)", e)

    # Prepare ffmpeg headers string (CRLF between headers)
    headers = "Referer: https://kick.com\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)\r\n"

    # Build ffmpeg command; -y to overwrite temp if present (but we will not overwrite final file)
    cmd = [
        ffmpeg_bin,
        "-hide_banner",
        "-loglevel", "info",
        "-headers", headers,
        "-i", stream_url,
        "-c", "copy",
        "-bsf:a", "aac_adtstoasc",
        "-f", "mp4",
        temp_path,
        "-y"
    ]

    attempt = 0
    while attempt <= max_retries:
        attempt += 1
        log.info("ffmpeg attempt %d/%d for %s", attempt, max_retries + 1, safe_title)
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            # stream stderr so we can log progress lines incrementally
            assert proc.stderr is not None
            last_err = b""
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                last_err = line
                try:
                    text = line.decode(errors="ignore").strip()
                    # lightweight parse for progress-like info (time=...)
                    if "time=" in text or "frame=" in text or "size=" in text:
                        log.debug("ffmpeg: %s", text)
                except Exception:
                    pass

            await proc.wait()
            rc = proc.returncode

            if rc == 0 and os.path.exists(temp_path) and os.path.getsize(temp_path) > 1024:
                # move temp to final atomically
                try:
                    os.replace(temp_path, out_mp4)
                except Exception:
                    # fallback to copy+remove
                    shutil.copyfile(temp_path, out_mp4)
                    try:
                        os.remove(temp_path)
                    except Exception:
                        pass
                log.info("Download finished: %s", out_mp4)
                return True, out_mp4
            else:
                # Non-zero rc or no meaningful file -> transient failure
                stderr_text = ""
                try:
                    stderr_text = last_err.decode(errors="ignore")
                except Exception:
                    pass
                log.warning("ffmpeg failed (rc=%s). Last stderr: %s", rc, stderr_text[:400])
                # Do not delete temp_path immediately; keep for inspection/resume attempts.
        except asyncio.CancelledError:
            log.info("Download cancelled for %s", safe_title)
            # do not delete temp file; allow retry later
            return False, None
        except Exception as exc:
            log.exception("Unexpected exception during ffmpeg download: %s", exc)

        # Backoff before retrying
        if attempt <= max_retries:
            backoff = retry_backoff * (2 ** (attempt - 1))
            log.info("Retrying in %.1f seconds...", backoff)
            await asyncio.sleep(backoff)
        else:
            break

    log.error("All ffmpeg attempts failed for %s", safe_title)
    return False, None

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
                ok = await download_vod(stream_url, dl_path, title)
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
