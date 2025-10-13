#!/usr/bin/env python3
"""
app.py - Transparent S3 proxy with logging for MinIO backend.

Usage:
  MINIO_TARGET=http://192.168.192.200:9000 python app.py

This proxy will listen on 0.0.0.0:5001 by default and proxy requests to MINIO_TARGET,
preserving Authorization and Host headers so SigV4 remains valid.
"""
import os
import uuid
import time
import logging
import tempfile
from pathlib import Path
from flask import Flask, request, Response, abort
import requests 
import json
from logging.handlers import RotatingFileHandler
from prometheus_client import Counter, Histogram, generate_latest

# ---------- CONFIG ----------
MINIO_TARGET = os.environ.get("MINIO_TARGET", "http://192.168.192.115:9000")
LOG_DIR = Path(os.environ.get("S3_PROXY_LOG_DIR", "./logged_requests"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

MAX_IN_MEM = 10 * 1024 * 1024  # 10 MB
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

# Reuse requests.Session for connection pooling
session = requests.Session()

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
    ts = time.strftime("%Y%m%dT%H%M%S")
    body_fname = LOG_DIR / f"{ts}_{req_id}.body"
    with open(body_fname, "wb") as bf:
        bf.write(data_bytes)
    return str(body_fname)

def _read_request_body_spooled():
    spooled = tempfile.SpooledTemporaryFile(max_size=MAX_IN_MEM)
    total = 0
    chunk_size = 64 * 1024
    while True:
        chunk = request.stream.read(chunk_size)
        if not chunk:
            break
        spooled.write(chunk)
        total += len(chunk)
    spooled.seek(0)
    if total <= MAX_IN_MEM:
        body_bytes = spooled.read()
        spooled.close()
        saved_path = _save_body_to_tempfile(_unique_id(), body_bytes)
        return ("bytes", body_bytes, total, saved_path)
    else:
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
        fileobj = open(dest_path, "rb")
        return ("fileobj", fileobj, total, str(dest_path))

def _build_target_url():
    """
    Build the target URL on MINIO_TARGET preserving the original request path and query.
    If the incoming path has a '/minio' prefix (because clients pointed to /minio), we strip
    exactly that prefix while preserving the leading slash for the backend canonical request.
    """
    # original path includes leading '/'
    path = request.path or "/"
    # remove leading '/minio' component if present (but keep leading slash)
    if path.startswith("/minio/"):
        path = path[len("/minio"):]  # results in e.g. '/bucket/obj'
    elif path == "/minio":
        path = "/"  # treat '/minio' as root for backend
    # ensure MINIO_TARGET and path join with single slash
    base = MINIO_TARGET.rstrip("/")
    if not path.startswith("/"):
        path = "/" + path
    query = request.query_string.decode() if request.query_string else ""
    if query:
        return f"{base}{path}?{query}"
    else:
        return f"{base}{path}"

def _filter_response_headers(resp_headers):
    headers = []
    for k, v in resp_headers.items():
        if k.lower() in HOP_BY_HOP:
            continue
        headers.append((k, v))
    return headers

@app.route("/healthz")
def healthz():
    return "ok", 200

@app.route("/metrics")
def metrics():
    return generate_latest(), 200

# accept both root and /minio prefixed requests
@app.route("/", defaults={"path": ""}, methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"])
@app.route("/<path:path>", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"])
@app.route("/minio", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"])
@app.route("/minio/<path:path>", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"])
def proxy_minio(path=None):
    req_id = _unique_id()
    ts = time.strftime("%Y-%m-%d %H:%M:%S")

    try:
        body_kind, body_or_fileobj, total_len, saved_body_path = _read_request_body_spooled()
    except Exception as e:
        logger.exception("Error reading request body")
        return abort(500, description=f"Failed to read request body: {e}")

    meta = {
        "id": req_id,
        "timestamp": ts,
        "client_addr": request.remote_addr,
        "method": request.method,
        "full_path": request.full_path,
        "content_length": total_len,
    }
    headers_dict = {k: v for k, v in request.headers.items()}

    try:
        _save_request_log(req_id, meta, headers_dict, saved_body_path)
    except Exception:
        logger.exception("Failed to save request log")

    # forward all headers as-is (including Authorization and Host)
    forward_headers = dict(request.headers.items())

    # Build target URL
    target_url = _build_target_url()
    logger.info("Proxying request id=%s %s %s -> %s", req_id, request.method, request.full_path, target_url)

    # perform the forwarded request using session
    try:
        if body_kind == "bytes":
            resp = session.request(
                method=request.method,
                url=target_url,
                headers=forward_headers,
                data=body_or_fileobj,
                allow_redirects=False,
                stream=True,
                timeout=None,
            )
        else:
            fileobj = body_or_fileobj
            try:
                fileobj.seek(0)
            except Exception:
                pass
            resp = session.request(
                method=request.method,
                url=target_url,
                headers=forward_headers,
                data=fileobj,
                allow_redirects=False,
                stream=True,
                timeout=None,
            )
    except requests.RequestException as e:
        logger.exception("Error forwarding request to backend")
        return abort(502, description=f"Backend forwarding error: {e}")

    # save backend error body for diagnostics if status >= 400
    try:
        if resp.status_code >= 400:
            debug_fname = LOG_DIR / f"{req_id}_backend_response.txt"
            try:
                # resp.text is safe for small response bodies (error XMLs typically are small)
                body_text = resp.text
                with open(debug_fname, "w", encoding="utf-8") as df:
                    df.write(body_text)
            except Exception:
                logger.exception("Failed to save backend response body")
            logger.info("Saved backend response for req %s -> %s", req_id, debug_fname)
    except Exception:
        logger.exception("Failed to process backend response body")

    def generate():
        try:
            for chunk in resp.iter_content(chunk_size=RESPONSE_CHUNK_SIZE):
                if chunk:
                    yield chunk
        finally:
            try:
                resp.close()
            except Exception:
                pass
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
    app.run(host=bind_host, port=bind_port, threaded=True)
