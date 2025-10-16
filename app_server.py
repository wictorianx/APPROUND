#!/usr/bin/env python3
"""
APPROUND Flask server + async scheduler backbone.

- Run: python app_server.py
- Exposes:
  GET /jobs    -> JSON list of jobs
  GET /logs    -> SSE stream of live events
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
from typing import Optional

from flask import Flask, Response, jsonify, stream_with_context
from kickapi import KickAPI  # your working wrapper
# You already have google auth/uploader code elsewhere

# ---------------------------
# Config & globals
# ---------------------------
CONFIG_FILE = "config.json"
DB_FILE = "vod_jobs.db"
LOG_QUEUE_MAX = 1000

log = logging.getLogger("APPROUND")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Thread-safe queue for logs/events (used for SSE)
log_queue: "queue.Queue[dict]" = queue.Queue(maxsize=LOG_QUEUE_MAX)

# DB lock because we'll use sqlite3 (connections across threads)
DB_LOCK = threading.Lock()

# Flask app
app = Flask(__name__)

# ---------------------------
# DB helpers (synchronous)
# ---------------------------
def db_connect():
    # allow access across threads; but we guard with DB_LOCK
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def init_db():
    with DB_LOCK:
        db = db_connect()
        cur = db.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            streamer TEXT,
            vod_id TEXT UNIQUE,
            title TEXT,
            stream_url TEXT,
            status TEXT,
            progress TEXT,
            file_path TEXT,
            yt_link TEXT,
            error TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """)
        db.commit()
        db.close()
    log.info("DB initialized: %s", os.path.abspath(DB_FILE))

def add_job_sync(streamer: str, vod_id: str, title: str, stream_url: Optional[str]):
    now = datetime.datetime.utcnow().isoformat()
    with DB_LOCK:
        db = db_connect()
        try:
            db.execute(
                "INSERT OR IGNORE INTO jobs (streamer, vod_id, title, stream_url, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (streamer, vod_id, title, stream_url or None, "queued", now, now)
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
            cur = db.execute("SELECT vod_id, streamer, title, status, progress, file_path, yt_link, error FROM jobs WHERE status = ? ORDER BY created_at", (status,))
        else:
            cur = db.execute("SELECT vod_id, streamer, title, status, progress, file_path, yt_link, error FROM jobs ORDER BY created_at")
        rows = cur.fetchall()
        db.close()
    keys = ["vod_id","streamer","title","status","progress","file_path","yt_link","error"]
    return [dict(zip(keys, r)) for r in rows]

# ---------------------------
# Logging / SSE helpers
# ---------------------------
def push_log(event: dict):
    """Push an event dict to the queue; non-blocking if full."""
    try:
        log_queue.put_nowait(event)
    except queue.Full:
        # drop oldest then push
        try:
            _ = log_queue.get_nowait()
            log_queue.put_nowait(event)
        except Exception:
            pass

@app.route("/jobs")
def route_jobs():
    jobs = fetch_jobs_sync()
    return jsonify(jobs)

@app.route("/logs")
def route_logs():
    def gen():
        # stream SSE events; block on queue.get()
        while True:
            try:
                ev = log_queue.get()
            except Exception:
                break
            # ev must be JSON-serializable
            try:
                msg = json.dumps(ev, default=str)
            except Exception:
                msg = json.dumps({"msg": str(ev)})
            yield f"data: {msg}\n\n"
    return Response(stream_with_context(gen()), mimetype="text/event-stream")

# ---------------------------
# Downloader (async, ffmpeg)
# ---------------------------
async def download_vod_ffmpeg(stream_url: str, output_dir: str, title: str,
                              min_free_gb: float = 1.5, max_retries: int = 3) -> (bool, Optional[str]):
    """
    Robust ffmpeg-based download function.
    Emits progress events via push_log({'type':'progress', ...})
    Returns (success, path_or_none)
    """
    safe = "".join(c for c in title if c.isalnum() or c in (" ", "_", "-", ".")).strip() or "vod"
    out_mp4 = os.path.join(output_dir, f"{safe}.mp4")
    temp = out_mp4 + ".part"
    os.makedirs(output_dir, exist_ok=True)

    # quick skip
    if os.path.exists(out_mp4) and os.path.getsize(out_mp4) > 1024:
        push_log({"type":"info","msg":"already exists","title":title,"path":out_mp4})
        return True, out_mp4

    # disk check
    try:
        free_gb = shutil.disk_usage(output_dir).free / (1024**3)
        if free_gb < min_free_gb:
            push_log({"type":"error","msg":"low_disk","free_gb":free_gb})
            return False, None
    except Exception as e:
        push_log({"type":"warn","msg":"disk_check_failed","error":str(e)})

    headers = "Referer: https://kick.com\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)\r\n"
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "info",
        "-headers", headers,
        "-i", stream_url,
        "-c", "copy", "-bsf:a", "aac_adtstoasc",
        "-f", "mp4", temp, "-y"
    ]

    attempt = 0
    while attempt <= max_retries:
        attempt += 1
        push_log({"type":"info","msg":"ffmpeg_start","title":title,"attempt":attempt})
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        assert proc.stderr is not None
        last_time = None
        # parse stderr lines for time=... to emit progress (time only)
        while True:
            line = await proc.stderr.readline()
            if not line:
                break
            text = line.decode(errors="ignore").strip()
            # look for time=HH:MM:SS.xx
            if "time=" in text:
                import re
                m = re.search(r"time=(\d+:\d+:\d+\.\d+)", text)
                if m:
                    last_time = m.group(1)
                    push_log({"type":"progress","title":title,"time":last_time,"msg":"ffmpeg_time"})
            # forward important stderr lines to logs (rate-limited could be applied)
            if "error" in text.lower() or "failed" in text.lower():
                push_log({"type":"ffmpeg_stderr","title":title,"line":text})

        rc = await proc.wait()
        if rc == 0 and os.path.exists(temp) and os.path.getsize(temp) > 1024:
            # finalize
            try:
                os.replace(temp, out_mp4)
            except Exception:
                shutil.copyfile(temp, out_mp4)
                try: os.remove(temp)
                except: pass
            push_log({"type":"done","title":title,"path":out_mp4})
            return True, out_mp4
        else:
            push_log({"type":"warn","msg":"ffmpeg_failed","title":title,"rc":rc, "attempt": attempt})
            # keep temp file for inspection / future attempts (pseudo-resume)
            if attempt <= max_retries:
                await asyncio.sleep(3 * attempt)
            else:
                push_log({"type":"error","msg":"ffmpeg_all_attempts_failed","title":title})
                return False, None

# ---------------------------
# Scheduler & watcher (async)
# ---------------------------
async def async_fetch_new_vods(watchlist):
    """
    Poll KickAPI inside a thread (kickapi is sync) via asyncio.to_thread
    and add any new jobs to DB.
    """
    api = KickAPI()
    for user in watchlist:
        try:
            # channel and v list are sync calls; run in thread to not block loop
            chan = await asyncio.to_thread(api.channel, user)
            vids = await asyncio.to_thread(lambda: list(chan.videos))
            for v in vids:
                vod_id = v.id
                title = v.title or f"{user}_{vod_id}"
                stream_url = getattr(v, "stream", None)
                if not stream_url:
                    # not ready yet
                    continue
                if not await asyncio.to_thread(job_exists_sync, vod_id):
                    await asyncio.to_thread(add_job_sync, user, vod_id, title, stream_url)
                    push_log({"type":"info","msg":"new_vod","streamer":user,"vod_id":vod_id,"title":title})
        except Exception as e:
            push_log({"type":"error","msg":"watcher_error","streamer":user,"error":str(e)})

async def scheduler_loop(config):
    """
    Main async loop: poll, download queued jobs, then upload (upload code not included here).
    """
    dl_path = config.get("download_path", "./downloads")
    interval = config.get("interval_minutes", 6)
    sem_dl = asyncio.Semaphore(config.get("max_concurrent_downloads", 1))

    while True:
        try:
            push_log({"type":"info","msg":"cycle_start"})
            await async_fetch_new_vods(config.get("watchlist", []))

            # fetch queued jobs (sync DB call in thread)
            queued = await asyncio.to_thread(fetch_jobs_sync, "queued")

            for job in queued:
                vod_id = job["vod_id"]
                streamer = job["streamer"]
                title = job["title"]
                stream_url = job.get("stream_url")
                if not stream_url:
                    # try refresh from Kick
                    push_log({"type":"info","msg":"refresh_stream","vod_id":vod_id})
                    try:
                        api = KickAPI()
                        chan = await asyncio.to_thread(api.channel, streamer)
                        vids = await asyncio.to_thread(lambda: list(chan.videos))
                        for v in vids:
                            if v.id == vod_id:
                                stream_url = getattr(v, "stream", None)
                                if stream_url:
                                    await asyncio.to_thread(update_job_sync, vod_id, stream_url=stream_url)
                                break
                    except Exception as e:
                        push_log({"type":"warn","msg":"refresh_failed","vod_id":vod_id,"error":str(e)})
                if not stream_url:
                    continue

                # Check disk space
                free_gb = shutil.disk_usage(dl_path).free / (1024**3)
                if free_gb < 1.0:
                    push_log({"type":"warn","msg":"low_disk_skip","free_gb":free_gb})
                    break

                # claim and download
                # set status to downloading
                await asyncio.to_thread(update_job_sync, vod_id, status="downloading")
                async with sem_dl:
                    push_log({"type":"info","msg":"start_download","vod_id":vod_id,"title":title})
                    ok, path = await download_vod_ffmpeg(stream_url, dl_path, title)
                    if ok:
                        await asyncio.to_thread(update_job_sync, vod_id, status="downloaded", file_path=path)
                        push_log({"type":"info","msg":"downloaded","vod_id":vod_id,"path":path})
                    else:
                        # put back to queued to retry later
                        await asyncio.to_thread(update_job_sync, vod_id, status="queued", error="download_failed")
                        push_log({"type":"warn","msg":"download_retry_scheduled","vod_id":vod_id})
            push_log({"type":"info","msg":"cycle_complete"})
        except Exception as e:
            push_log({"type":"error","msg":"scheduler_exception","error":str(e)})
        await asyncio.sleep(interval)

# ---------------------------
# Background runner thread (runs the asyncio loop)
# ---------------------------
def start_background_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(scheduler_loop(CONFIG))

# ---------------------------
# Entrypoint
# ---------------------------
if __name__ == "__main__":
    if not os.path.exists(CONFIG_FILE):
        print("Missing config.json (create with watchlist, download_path, interval_minutes).")
        sys.exit(1)

    with open(CONFIG_FILE, encoding="utf-8") as f:
        CONFIG = json.load(f)

    init_db()

    # start asyncio scheduler in separate thread
    new_loop = asyncio.new_event_loop()
    t = threading.Thread(target=start_background_loop, args=(new_loop,), daemon=True)
    t.start()
    log.info("Scheduler background thread started.")

    # run Flask app in main thread
    # by default listens on 127.0.0.1:5000
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
