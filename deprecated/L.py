#!/usr/bin/env python3
"""
APPROUND Overhaul (drop-in separate file)

- Purpose: simplified, robust server + scheduler that uses Kick's direct API for VODs.
- How to use:
    1. Save as `app_overhaul_simple.py` next to your existing project.
    2. Create `config.json` with a "vod_urls" list or "watchlist" (see example below).
    3. Run: python app_overhaul_simple.py
- Endpoints:
    GET /jobs    -> JSON list of jobs
    GET /logs    -> SSE live events
    GET /quota   -> quota usage
    POST /control/retry/<vod_id> -> requeue a job
"""

import os
import sys
import json
import time
import sqlite3
import threading
import asyncio
import queue
import logging
import datetime
import shutil
import re
from typing import Optional, Tuple
from urllib.parse import urlparse

import requests
from flask import Flask, Response, jsonify, stream_with_context, request

# -------------------------
# Config / constants
# -------------------------
CONFIG_FILE = "config.json"
DB_FILE = "vod_jobs.db"
LOG_QUEUE_MAX = 2000

DEFAULT_MIN_FREE_GB = 2.0
DEFAULT_BITRATE_MBPS = 8.0
YOUTUBE_UNITS_PER_UPLOAD = 1600  # sample quota per upload
YOUTUBE_DAILY_UNITS = 10000

# -------------------------
# Logging + SSE queue
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("APPROUND")
log.addHandler(logging.StreamHandler(sys.stdout))

log_queue: "queue.Queue[dict]" = queue.Queue(maxsize=LOG_QUEUE_MAX)


def push_log(event: dict):
    """Emit event to console and to SSE queue (non-blocking)."""
    ts = datetime.datetime.utcnow().isoformat()
    event_out = {"ts": ts, **event}
    # Console short print
    t = event.get("type", "log").upper()
    msg = event.get("msg") or event.get("message") or ""
    meta = []
    if "title" in event:
        meta.append(event["title"])
    if "vod_id" in event:
        meta.append(event["vod_id"])
    meta_s = " ".join(str(m) for m in meta) if meta else ""
    print(f"[{t}] {msg} {meta_s}", flush=True)
    # queue for SSE
    try:
        log_queue.put_nowait(event_out)
    except queue.Full:
        try:
            _ = log_queue.get_nowait()
            log_queue.put_nowait(event_out)
        except Exception:
            pass


# -------------------------
# DB helpers (synchronous)
# -------------------------
DB_LOCK = threading.Lock()


def db_connect() -> sqlite3.Connection:
    return sqlite3.connect(DB_FILE, check_same_thread=False)


def init_db():
    """Initialize tables: jobs + quota"""
    with DB_LOCK:
        db = db_connect()
        cur = db.cursor()
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            streamer TEXT,
            vod_id TEXT UNIQUE,
            title TEXT,
            stream_url TEXT,
            duration TEXT,
            status TEXT,
            attempts INTEGER DEFAULT 0,
            progress TEXT,
            file_path TEXT,
            yt_link TEXT,
            error TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
        )
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS quota (
            date TEXT PRIMARY KEY,
            used_units INTEGER DEFAULT 0
        )
        """
        )
        db.commit()
        db.close()
    push_log({"type": "info", "msg": "db_initialized", "path": os.path.abspath(DB_FILE)})


def add_job_sync(streamer: str, vod_uuid: str, title: str, stream_url: Optional[str], duration: Optional[str]):
    now = datetime.datetime.utcnow().isoformat()
    with DB_LOCK:
        db = db_connect()
        try:
            db.execute(
                "INSERT OR IGNORE INTO jobs (streamer, vod_id, title, stream_url, duration, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (streamer, vod_uuid, title, stream_url, duration, "queued", now, now),
            )
            db.commit()
        finally:
            db.close()


def job_exists_sync(vod_id: str) -> bool:
    with DB_LOCK:
        db = db_connect()
        cur = db.execute("SELECT 1 FROM jobs WHERE vod_id = ? LIMIT 1", (vod_id,))
        r = cur.fetchone()
        db.close()
        return r is not None


def update_job_sync(vod_id: str, **kwargs):
    if not kwargs:
        return
    cols = ", ".join(f"{k} = ?" for k in kwargs.keys())
    values = list(kwargs.values())
    values.append(datetime.datetime.utcnow().isoformat())
    values.append(vod_id)
    with DB_LOCK:
        db = db_connect()
        db.execute(f"UPDATE jobs SET {cols}, updated_at = ? WHERE vod_id = ?", values)
        db.commit()
        db.close()


def fetch_jobs_sync(status: Optional[str] = None):
    with DB_LOCK:
        db = db_connect()
        if status:
            cur = db.execute(
                "SELECT vod_id, streamer, title, stream_url, duration, status, attempts, progress, file_path FROM jobs WHERE status = ? ORDER BY created_at",
                (status,),
            )
        else:
            cur = db.execute(
                "SELECT vod_id, streamer, title, stream_url, duration, status, attempts, progress, file_path FROM jobs ORDER BY created_at"
            )
        rows = cur.fetchall()
        db.close()
    keys = ["vod_id", "streamer", "title", "stream_url", "duration", "status", "attempts", "progress", "file_path"]
    return [dict(zip(keys, r)) for r in rows]


# -------------------------
# Quota helpers
# -------------------------
def register_upload(units: int = YOUTUBE_UNITS_PER_UPLOAD):
    today = datetime.date.today().isoformat()
    with DB_LOCK:
        db = db_connect()
        db.execute("INSERT OR IGNORE INTO quota (date, used_units) VALUES (?, 0)", (today,))
        db.execute("UPDATE quota SET used_units = used_units + ? WHERE date = ?", (units, today))
        db.commit()
        db.close()
    push_log({"type": "info", "msg": "quota_registered", "units": units})


def quota_remaining() -> int:
    today = datetime.date.today().isoformat()
    with DB_LOCK:
        db = db_connect()
        cur = db.execute("SELECT used_units FROM quota WHERE date = ?", (today,))
        row = cur.fetchone()
        db.close()
    used = row[0] if row else 0
    return YOUTUBE_DAILY_UNITS - int(used)


def get_quota_usage() -> int:
    today = datetime.date.today().isoformat()
    with DB_LOCK:
        db = db_connect()
        cur = db.execute("SELECT used_units FROM quota WHERE date = ?", (today,))
        row = cur.fetchone()
        db.close()
    return int(row[0]) if row else 0


# -------------------------
# Kick helper: extract uuid from URL, fetch metadata
# -------------------------
UUID_RE = re.compile(r"[0-9a-fA-F-]{8,}")


def extract_uuid_from_kick_url(url: str) -> Optional[str]:
    try:
        p = urlparse(url)
        parts = [pp for pp in p.path.split("/") if pp]
        # expected: /<channel>/videos/<uuid>
        if len(parts) >= 3 and parts[-2] == "videos":
            candidate = parts[-1]
            if UUID_RE.search(candidate):
                return candidate
    except Exception:
        pass
    return None


from kickapi import KickAPI
import requests

def get_vod_info_kickapi(user: str, vod_uuid: str):
    """
    Retrieve reliable VOD metadata using KickAPI first, then Kick REST fallback.
    Returns dict {uuid, title, stream_url, duration, streamer}.
    """
    api = KickAPI()
    try:
        chan = api.channel(user)
        vod = next((v for v in chan.videos if getattr(v, "uuid", "") == vod_uuid), None)
        if vod:
            return {
                "uuid": vod_uuid,
                "title": vod.title or f"{user}_{vod_uuid}",
                "stream_url": getattr(vod, "stream", None),
                "duration": getattr(vod, "duration", "01:00:00"),
                "streamer": user
            }
    except Exception as e:
        push_log({"type": "warn", "msg": "kickapi_primary_failed", "user": user, "error": str(e)})

    # fallback: direct API call
    try:
        r = requests.get(f"https://kick.com/api/v1/video/{vod_uuid}", timeout=10)
        if r.status_code == 200:
            data = r.json()
            return {
                "uuid": vod_uuid,
                "title": data.get("title") or f"{user}_{vod_uuid}",
                "stream_url": data.get("source_url") or data.get("hls_url"),
                "duration": data.get("duration") or data.get("vod_length") or "01:00:00",
                "streamer": user
            }
    except Exception as e:
        push_log({"type": "error", "msg": "kickapi_fallback_failed", "user": user, "error": str(e)})

    return None



# -------------------------
# Estimate size helper
# -------------------------
def estimate_vod_size_gb(duration_str: Optional[str], bitrate_mbps: float = DEFAULT_BITRATE_MBPS) -> float:
    if not duration_str:
        return 1.0
    try:
        parts = duration_str.split(":")
        if len(parts) == 3:
            h, m, s = map(int, parts)
        elif len(parts) == 2:
            h = 0
            m, s = map(int, parts)
        else:
            return 1.0
        seconds = h * 3600 + m * 60 + s
        mb = seconds * (bitrate_mbps / 8.0)
        gb = mb / 1024.0
        return gb
    except Exception:
        return 1.0


# -------------------------
# Downloader (ffmpeg) - async
# -------------------------
async def download_vod_ffmpeg(
    stream_url: str, output_dir: str, title: str, min_free_gb: float = DEFAULT_MIN_FREE_GB, max_retries: int = 3
) -> Tuple[bool, Optional[str]]:
    """Download HLS via ffmpeg -> .part then .mp4. Returns (success, path)."""
    safe = "".join(c for c in title if c.isalnum() or c in (" ", "_", "-", ".")).strip() or "vod"
    out_mp4 = os.path.join(output_dir, f"{safe}.mp4")
    temp = out_mp4 + ".part"
    os.makedirs(output_dir, exist_ok=True)

    if os.path.exists(out_mp4) and os.path.getsize(out_mp4) > 1024:
        push_log({"type": "info", "msg": "already_downloaded", "title": title, "path": out_mp4})
        return True, out_mp4

    try:
        free_gb = shutil.disk_usage(output_dir).free / (1024 ** 3)
        if free_gb < min_free_gb:
            push_log({"type": "error", "msg": "low_disk", "free_gb": free_gb, "title": title})
            return False, None
    except Exception as e:
        push_log({"type": "warn", "msg": "disk_check_failed", "error": str(e), "title": title})

    headers = "Referer: https://kick.com\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)\r\n"
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "info",
        "-headers",
        headers,
        "-i",
        stream_url,
        "-c",
        "copy",
        "-bsf:a",
        "aac_adtstoasc",
        "-f",
        "mp4",
        temp,
        "-y",
    ]

    attempt = 0
    while attempt <= max_retries:
        attempt += 1
        push_log({"type": "info", "msg": "ffmpeg_start", "title": title, "attempt": attempt})
        try:
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            assert proc.stderr is not None
            last_time = None
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                text = line.decode(errors="ignore").strip()
                # parse time=XX:XX:XX.xx
                m = re.search(r"time=(\d+:\d+:\d+\.\d+)", text)
                if m:
                    last_time = m.group(1)
                    push_log({"type": "progress", "title": title, "time": last_time})
                if "error" in text.lower() or "failed" in text.lower():
                    push_log({"type": "ffmpeg_err", "title": title, "line": text})
            rc = await proc.wait()
            if rc == 0 and os.path.exists(temp) and os.path.getsize(temp) > 1024:
                try:
                    os.replace(temp, out_mp4)
                except Exception:
                    shutil.copyfile(temp, out_mp4)
                    try:
                        os.remove(temp)
                    except Exception:
                        pass
                push_log({"type": "done", "msg": "download_complete", "title": title, "path": out_mp4})
                return True, out_mp4
            else:
                push_log({"type": "warn", "msg": "ffmpeg_failed", "title": title, "rc": rc})
        except asyncio.CancelledError:
            push_log({"type": "warn", "msg": "ffmpeg_cancelled", "title": title})
            return False, None
        except Exception as exc:
            push_log({"type": "error", "msg": "ffmpeg_exception", "title": title, "error": str(exc)})

        if attempt <= max_retries:
            backoff = 3 * attempt
            push_log({"type": "info", "msg": "ffmpeg_backoff", "title": title, "sleep": backoff})
            await asyncio.sleep(backoff)
        else:
            break

    push_log({"type": "error", "msg": "ffmpeg_all_attempts_failed", "title": title})
    return False, None


# -------------------------
# Scheduler: queue VODs directly from URLs, then download
# -------------------------
async def async_queue_vods_from_urls(vod_urls):
    """Given a list of Kick VOD URLs, fetch metadata and queue them."""
    for url in vod_urls:
        info = await asyncio.to_thread(get_vod_info_from_url, url)
        if not info:
            push_log({"type": "warn", "msg": "vod_info_failed", "url": url})
            continue
        vod_id = info["uuid"]
        streamer = info.get("user") or "unknown"
        if not await asyncio.to_thread(job_exists_sync, vod_id):
            await asyncio.to_thread(add_job_sync, streamer, vod_id, info["title"], info["stream_url"], info.get("duration"))
            push_log({"type": "info", "msg": "queued_vod", "vod_id": vod_id, "title": info["title"]})


async def scheduler_loop(config):
    dl_path = config.get("download_path", "./downloads")
    os.makedirs(dl_path, exist_ok=True)
    interval_sec = int(config.get("interval_minutes", 6)) * 60
    min_free_gb = float(config.get("min_free_gb", DEFAULT_MIN_FREE_GB))
    bitrate = float(config.get("bitrate_mbps", DEFAULT_BITRATE_MBPS))
    max_concurrent_downloads = int(config.get("max_concurrent_downloads", 1))
    sem_dl = asyncio.Semaphore(max_concurrent_downloads)

    push_log({"type": "info", "msg": "scheduler_started"})
    while True:
        try:
            push_log({"type": "info", "msg": "cycle_start"})
            # Queue any direct vod URLs if provided
            vod_urls = config.get("vod_urls") or config.get("watchlist") or []
            if vod_urls:
                await async_queue_vods_from_urls(vod_urls)

            # prioritize uploads first (stub - handled elsewhere)
            remaining_units = await asyncio.to_thread(quota_remaining)
            push_log({"type": "info", "msg": "quota_remaining", "remaining": remaining_units})

            # handle downloads
            queued = await asyncio.to_thread(fetch_jobs_sync, "queued")
            for job in queued:
                vod_id = job["vod_id"]
                title = job["title"]
                stream_url = job.get("stream_url")
                duration = job.get("duration")

                if not stream_url:
                    # Last-ditch fetch from API (in case stored was None)
                    push_log({"type": "info", "msg": "refresh_stream_url", "vod_id": vod_id})
                    stream_url = await asyncio.to_thread(get_vod_info_from_url, f"https://kick.com/{job['streamer']}/videos/{vod_id}")
                    if isinstance(stream_url, dict):
                        stream_url = stream_url.get("stream_url")
                    if stream_url:
                        await asyncio.to_thread(update_job_sync, vod_id, stream_url=stream_url)
                    else:
                        push_log({"type": "warn", "msg": "no_stream_available", "vod_id": vod_id})
                        continue

                est_gb = estimate_vod_size_gb(duration, bitrate)
                free_gb = shutil.disk_usage(dl_path).free / (1024 ** 3)
                if free_gb < est_gb + min_free_gb:
                    push_log({"type": "warn", "msg": "not_enough_disk", "free_gb": free_gb, "required_gb": est_gb + min_free_gb})
                    break

                # claim -> downloading
                await asyncio.to_thread(update_job_sync, vod_id, status="downloading", attempts=job.get("attempts", 0) + 1)
                async with sem_dl:
                    push_log({"type": "info", "msg": "start_download", "vod_id": vod_id, "title": title})
                    ok, path = await download_vod_ffmpeg(stream_url, dl_path, title, min_free_gb=min_free_gb)
                    if ok:
                        await asyncio.to_thread(update_job_sync, vod_id, status="downloaded", file_path=path)
                        push_log({"type": "info", "msg": "downloaded", "vod_id": vod_id, "path": path})
                    else:
                        attempts = job.get("attempts", 0) + 1
                        await asyncio.to_thread(update_job_sync, vod_id, status="queued", attempts=attempts, error="download_failed")
                        push_log({"type": "warn", "msg": "download_failed_requeued", "vod_id": vod_id, "attempts": attempts})
            push_log({"type": "info", "msg": "cycle_complete"})
        except Exception as e:
            push_log({"type": "error", "msg": "scheduler_exception", "error": str(e)})
        await asyncio.sleep(interval_sec)


# -------------------------
# Simple uploader stub (respects quota)
# -------------------------
def uploader_try_upload(file_path: str, title: str, privacy: str = "private") -> Optional[str]:
    if not os.path.exists(file_path):
        push_log({"type": "error", "msg": "upload_missing_file", "title": title})
        return None
    if quota_remaining() < YOUTUBE_UNITS_PER_UPLOAD:
        push_log({"type": "warn", "msg": "upload_quota_insufficient", "title": title})
        return None
    # Simulate upload and consume quota
    register_upload()
    fake = f"https://youtu.be/FAKE_{int(time.time())}"
    push_log({"type": "info", "msg": "upload_simulated", "title": title, "yt": fake})
    return fake


# -------------------------
# Flask app & endpoints
# -------------------------
app = Flask(__name__)


@app.route("/jobs", methods=["GET"])
def route_jobs():
    return jsonify(fetch_jobs_sync())


@app.route("/logs", methods=["GET"])
def route_logs():
    def gen():
        while True:
            try:
                ev = log_queue.get()
            except Exception:
                break
            try:
                yield f"data: {json.dumps(ev, default=str)}\n\n"
            except Exception:
                yield f"data: {json.dumps({'msg': str(ev)})}\n\n"

    return Response(stream_with_context(gen()), mimetype="text/event-stream")


@app.route("/quota", methods=["GET"])
def route_quota():
    return jsonify({"used_units": get_quota_usage(), "remaining_units": quota_remaining()})


@app.route("/control/retry/<vod_id>", methods=["POST"])
def route_retry(vod_id):
    if not job_exists_sync(vod_id):
        return jsonify({"ok": False, "error": "unknown vod_id"}), 404
    update_job_sync(vod_id, status="queued", error=None)
    push_log({"type": "info", "msg": "manual_retry", "vod_id": vod_id})
    return jsonify({"ok": True})


# -------------------------
# Background start + main
# -------------------------
def start_background_loop(loop, cfg):
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(scheduler_loop(cfg))
    except Exception as e:
        push_log({"type": "error", "msg": "background_crash", "error": str(e)})
        import traceback
        traceback.print_exc()


def main():
    if not os.path.exists(CONFIG_FILE):
        print("Create config.json (keys: vod_urls or watchlist, download_path, interval_minutes, min_free_gb, bitrate_mbps).")
        sys.exit(1)
    with open(CONFIG_FILE, encoding="utf-8") as f:
        cfg = json.load(f)

    init_db()

    new_loop = asyncio.new_event_loop()
    t = threading.Thread(target=start_background_loop, args=(new_loop, cfg), daemon=True)
    t.start()
    push_log({"type": "info", "msg": "scheduler_thread_started"})

    # Run Flask in main thread
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)


if __name__ == "__main__":
    main()
