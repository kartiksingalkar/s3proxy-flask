#!/usr/bin/env python3
"""
app.py - Transparent S3 proxy with logging for MinIO backend.

Usage:
  MINIO_TARGET=http://192.168.192.200:9000 python app.py

This proxy will listen on 0.0.0.0:5001 by default and proxy requests made to /minio...
to the MINIO_TARGET (keeping headers including Authorization intact).
"""
import os
import uuid
import time
import logging
import tempfile
from pathlib import Path
from flask import Flask, request, Response, abort
import requests

import logging, json
from logging.handlers import RotatingFileHandler
from prometheus_client import Counter, Histogram, generate_latest


# ---------- CONFIG ----------
# Destination MinIO server (single backend). Must include scheme and optional port.
# Example: "http://192.168.192.200:9000"
MINIO_TARGET = os.environ.get("MINIO_TARGET", "https://play.min.io:9000")

# Where to store logs and bodies
LOG_DIR = Path(os.environ.get("S3_PROXY_LOG_DIR", "./logged_requests"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

# For in-memory buffering before spooling to disk (bytes)
MAX_IN_MEM = 10 * 1024 * 1024  # 10 MB

# Chunk size for streaming response back to client
RESPONSE_CHUNK_SIZE = 64 * 1024

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("s3proxy")
handler = RotatingFileHandler("proxy.log", maxBytes=50_000_000, backupCount=10)
handler.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(handler)

# Metrics
REQ_COUNT = Counter("s3proxy_requests_total", "Total requests", ["method", "status"])
REQ_LATENCY = Histogram("s3proxy_request_duration_seconds", "Latency")

def log_request_json(meta, headers):
    logger.info(json.dumps({"ts": meta["timestamp"], "id": meta["id"], "meta": meta, "headers": headers}))

app = Flask(__name__)

# Hop-by-hop headers which must not be forwarded from response to client
HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


def _unique_id():
    return uuid.uuid4().hex


def _save_request_log(req_id: str, meta: dict, headers: dict, body_path: str):
    """
    Save a human-readable text log including metadata, headers and path to saved body.
    """
    ts = time.strftime("%Y%m%dT%H%M%S")
    fname = LOG_DIR / f"{ts}_{req_id}.txt"
    with open(fname, "w", encoding="utf-8") as f:
        f.write(f"timestamp: {ts}\n")
        for k, v in meta.items():
            f.write(f"{k}: {v}\n")
        f.write("\nHeaders:\n")
        for k, v in headers.items():
            f.write(f"{k}: {v}\n")
        f.write("\nBody file: {}\n".format(body_path))
    logger.info(f"Saved request log {fname}")


def _save_body_to_tempfile(req_id: str, data_bytes: bytes):
    """
    Save request body to a file under LOG_DIR. Return path.
    """
    ts = time.strftime("%Y%m%dT%H%M%S")
    body_fname = LOG_DIR / f"{ts}_{req_id}.body"
    with open(body_fname, "wb") as bf:
        bf.write(data_bytes)
    return str(body_fname)


def _read_request_body_spooled():
    """
    Read incoming request body; use spooled tempfile when large.
    Returns tuple (body_source, body_bytes_or_fileobj, total_length, body_saved_path_or_None)
    - If small: body_bytes_or_fileobj is bytes and body_saved_path_or_None is path to saved file.
    - If large and spooled to disk: body_bytes_or_fileobj is a file-like object opened for rb,
      and body_saved_path_or_None is path to saved file.
    """
    # Try to get content length (may be None)
    content_length = request.content_length

    # Read raw data (this consumes WSGI input)
    # We'll stream-read in chunks to avoid huge memory spike
    # If content_length is None we'll still read until EOF
    spooled = tempfile.SpooledTemporaryFile(max_size=MAX_IN_MEM)
    total = 0
    # read in chunks
    chunk_size = 64 * 1024
    while True:
        chunk = request.stream.read(chunk_size)
        if not chunk:
            break
        spooled.write(chunk)
        total += len(chunk)

    spooled.seek(0)

    # Decide whether to return bytes or file-like
    if total <= MAX_IN_MEM:
        # small - read into bytes and close spooled
        body_bytes = spooled.read()
        spooled.close()
        saved_path = _save_body_to_tempfile(_unique_id(), body_bytes)
        return ("bytes", body_bytes, total, saved_path)
    else:
        # large - persist to disk file in LOG_DIR
        ts = time.strftime("%Y%m%dT%H%M%S")
        req_id = _unique_id()
        dest_path = LOG_DIR / f"{ts}_{req_id}.body"
        with open(dest_path, "wb") as out_f:
            spooled.seek(0)
            while True:
                chunk = spooled.read(chunk_size)
                if not chunk:
                    break
                out_f.write(chunk)
        spooled.close()
        # reopen as file object for streaming to backend
        fileobj = open(dest_path, "rb")
        return ("fileobj", fileobj, total, str(dest_path))


def _build_target_url():
    """
    Build target URL on MINIO_TARGET preserving the full path and query string from the client.
    For example, if client called /minio/mybucket/myobj?foo=bar, we will forward the *same* full path.
    """
    # request.full_path often ends with '?' if there's no query string, so construct cleanly:
    path = request.path  # includes '/minio' prefix used by clients
    path = request.path
    if path.startswith("/minio/"):
        path = path[len("/"):]
    elif path.startswith("/minio"):
        path = path[len("/"):]
    query = request.query_string.decode() if request.query_string else ""
    if query:
        return f"{MINIO_TARGET}{path}?{query}"
    else:
        return f"{MINIO_TARGET}{path}"


def _filter_response_headers(resp_headers):
    """
    Remove hop-by-hop headers.
    Return list of (k,v) pairs to set on Flask response.
    """
    headers = []
    for k, v in resp_headers.items():
        if k.lower() in HOP_BY_HOP:
            continue
        # Some backends may return 'content-encoding' which Flask / client's underlying transport
        # can handle. We pass them through.
        headers.append((k, v))
    return headers

@app.route("/healthz")
def healthz():
    return "ok", 200

@app.route("/metrics")
def metrics():
    return generate_latest(), 200

@app.route("/", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"])
@app.route("/minio", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"])
@app.route("/minio/", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"])
@app.route("/minio/<path:path>", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"])
def proxy_minio(path=None):
    """
    Main transparent proxy endpoint. Logs the incoming request to text file(s),
    then forwards it verbatim to MINIO_TARGET and streams the response back.
    """
    req_id = _unique_id()
    ts = time.strftime("%Y-%m-%d %H:%M:%S")

    # --- Read & save request body safely (spool to disk for large bodies)
    try:
        body_kind, body_or_fileobj, total_len, saved_body_path = _read_request_body_spooled()
    except Exception as e:
        logger.exception("Error reading request body")
        return abort(500, description=f"Failed to read request body: {e}")

    # --- Build metadata + headers for log file
    meta = {
        "id": req_id,
        "timestamp": ts,
        "client_addr": request.remote_addr,
        "method": request.method,
        "full_path": request.full_path,
        "content_length": total_len,
    }

    # Convert headers to a simple dict for saving
    # Flask's request.headers is case-insensitive mapping; convert to plain dict
    headers_dict = {k: v for k, v in request.headers.items()}

    # Save a readable log with headers and pointer to body file
    try:
        _save_request_log(req_id, meta, headers_dict, saved_body_path)
    except Exception:
        logger.exception("Failed to save request log")

    # --- Prepare headers to forward
    # We forward all headers as-is (including Authorization, Host), because client signed them.
    # But drop 'Content-Length' only if we're sending a file object and requests sets it automatically
    forward_headers = dict(request.headers.items())

    # NOTE: keep Host as received to preserve SigV4 canonical request semantics.
    # Some HTTP clients may override Host when connecting; requests will use headers if provided.

    # --- Build target URL (preserve the entire path + query)
    target_url = _build_target_url()
    logger.info("Proxying request id=%s %s %s -> %s", req_id, request.method, request.full_path, target_url)

    # --- Perform the actual forwarded request to MinIO
    try:
        # Choose correct 'data' for requests
        if body_kind == "bytes":
            data = body_or_fileobj
            # requests will set Content-Length appropriately using len(data)
            resp = requests.request(
                method=request.method,
                url=target_url,
                headers=forward_headers,
                params=None,
                data=data,
                allow_redirects=False,
                stream=True,
                timeout=None,
            )
            # try:
            #     # (existing code that does resp = requests.request(...))
            #     # << your existing requests.request call producing `resp` >>
            # except requests.RequestException as e:
            #     logger.exception("Error forwarding request to backend")
            #     return abort(502, description=f"Backend forwarding error: {e}")

            # # --- DEBUG: if backend returns an error, save its body for inspection
            # try:
            #     if resp.status_code >= 400:
            #         resp_text = None
            #         # try to read small response bodies safely
            #         try:
            #             resp_text = resp.text
            #         except Exception:
            #             # if it's streaming large content, read up to a reasonable limit
            #             resp_text = resp.raw.read(8192, decode_content=True)
            #         debug_fname = LOG_DIR / f"{req_id}_backend_response.txt"
            #         with open(debug_fname, "wb") as df:
            #             if isinstance(resp_text, str):
            #                 df.write(resp_text.encode("utf-8", errors="replace"))
            #             else:
            #                 df.write(resp_text or b"")
            #         logger.info("Saved backend response for req %s -> %s", req_id, debug_fname)
            # except Exception:
            #     logger.exception("Failed to save backend response body")

        else:
            # file object (large upload) - pass the file object as data (it will be streamed)
            fileobj = body_or_fileobj
            # Ensure file cursor is at beginning
            try:
                fileobj.seek(0)
            except Exception:
                pass
            resp = requests.request(
                method=request.method,
                url=target_url,
                headers=forward_headers,
                params=None,
                data=fileobj,
                allow_redirects=False,
                stream=True,
                timeout=None,
            )
    except requests.RequestException as e:
        logger.exception("Error forwarding request to backend")
        return abort(502, description=f"Backend forwarding error: {e}")

    # --- Build Flask response streaming the backend response body
    def generate():
        try:
            for chunk in resp.iter_content(chunk_size=RESPONSE_CHUNK_SIZE):
                if chunk:
                    yield chunk
        finally:
            # Close backend connection
            try:
                resp.close()
            except Exception:
                pass
            # Close fileobj if used
            if body_kind == "fileobj":
                try:
                    fileobj.close()
                except Exception:
                    pass

    resp_headers = _filter_response_headers(resp.headers)
    flask_resp = Response(generate(), status=resp.status_code, headers=resp_headers)
    return flask_resp


if __name__ == "__main__":
    bind_host = os.environ.get("LISTEN_HOST", "0.0.0.0")
    bind_port = int(os.environ.get("LISTEN_PORT", 5001))
    logger.info("Starting S3 proxy (forwarding to %s) on %s:%s", MINIO_TARGET, bind_host, bind_port)
    # Note: For production use, run behind a robust server (gunicorn) or use gunicorn's --workers.
    app.run(host=bind_host, port=bind_port, threaded=True)
