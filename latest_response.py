#!/usr/bin/env python3
"""
app.py - Transparent S3 proxy with response logging for MinIO backend.

Usage:
  MINIO_TARGET=http://192.168.192.200:9000 python app.py

This proxy will listen on 0.0.0.0:5001 by default and proxy requests to MINIO_TARGET,
preserving Authorization and Host headers so SigV4 remains valid.
It logs full request traces and saves response bodies + metadata per request.
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
RESPONSE_LOG_DIR = LOG_DIR / "responses"
RESPONSE_LOG_DIR.mkdir(parents=True, exist_ok=True)

MAX_IN_MEM = 10 * 1024 * 1024  # 10 MB
RESPONSE_CHUNK_SIZE = 64 * 1024

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("s3proxy")
handler = RotatingFileHandler("proxy.log", maxBytes=50_000_000, backupCount=10)
handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(handler)

# Metrics
REQ_COUNT = Counter("s3proxy_requests_total", "Total requests", ["method", "status"])
REQ_LATENCY = Histogram("s3proxy_request_duration_seconds", "Latency")

app = Flask(__name__)
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
    ts = time.strftime("response_%Y%m%dT%H%M%S")
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
        saved_path = _save_request_body_tempfile(_unique_id(), body_bytes)
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

# Note: fix helper function name collision from earlier snippet
def _save_request_body_tempfile(req_id: str, data_bytes: bytes):
    ts = time.strftime("%Y%m%dT%H%M%S")
    body_fname = LOG_DIR / f"{ts}_{req_id}.body"
    with open(body_fname, "wb") as bf:
        bf.write(data_bytes)
    return str(body_fname)

def _build_target_url():
    path = request.path or "/"
    if path.startswith("/minio/"):
        path = path[len("/minio"):]
    elif path == "/minio":
        path = "/"
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

    # read & spool request body
    try:
        body_kind, body_or_fileobj, total_len, saved_body_path = _read_request_body_spooled()
    except Exception as e:
        logger.exception("Error reading request body")
        return abort(500, description=f"Failed to read request body: {e}")

    # capture request details now (so we don't refer to `request` later)
    captured_request = {
        "id": req_id,
        "timestamp": ts,
        "client_addr": request.remote_addr,
        "method": request.method,
        "full_path": request.full_path,
        "content_length": total_len,
        "headers": dict(request.headers),
        "body_file": saved_body_path,
    }

    # save request log
    try:
        _save_request_log(req_id, captured_request, captured_request["headers"], saved_body_path)
    except Exception:
        logger.exception("Failed to save request log")

    forward_headers = dict(request.headers.items())
    target_url = _build_target_url()
    logger.info("Proxying request id=%s %s %s -> %s", req_id, request.method, request.full_path, target_url)

    # perform the forwarded request using session
    try:
        if body_kind == "bytes":
            data_to_send = body_or_fileobj
            resp = session.request(
                method=request.method,
                url=target_url,
                headers=forward_headers,
                data=data_to_send,
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

    # prepare response log paths and capture some immediate response metadata
    response_body_path = RESPONSE_LOG_DIR / f"{time.strftime('%Y%m%dT%H%M%S')}_{req_id}.body"
    response_meta_path = RESPONSE_LOG_DIR / f"{time.strftime('%Y%m%dT%H%M%S')}_{req_id}_meta.json"

    # capture response-level static info now (status, headers). body will be streamed/written.
    captured_response_static = {
        "status_code": resp.status_code,
        "reason": getattr(resp, "reason", None),
        "headers": dict(resp.headers.items()),
        "content_length_header": resp.headers.get("Content-Length"),
        "content_type": resp.headers.get("Content-Type"),
        "x_amz_request_id": resp.headers.get("x-amz-request-id") or resp.headers.get("x-minio-request-id"),
        "elapsed_seconds": resp.elapsed.total_seconds() if hasattr(resp, "elapsed") else None,
    }

    # open file to save response body
    try:
        resp_body_f = open(response_body_path, "wb")
    except Exception:
        resp_body_f = None

    # note: generate() will be executed after request context ends, so it must only use captured_* and resp
    # def generate():
    #     try:
    #         for chunk in resp.iter_content(chunk_size=RESPONSE_CHUNK_SIZE):
    #             if chunk:
    #                 if resp_body_f:
    #                     try:
    #                         resp_body_f.write(chunk)
    #                     except Exception:
    #                         logger.exception("Failed to write chunk to response body file")
    #                 yield chunk
    #     finally:
    #         # close backend response and any file handles
    #         try:
    #             resp.close()
    #         except Exception:
    #             pass
    #         if body_kind == "fileobj":
    #             try:
    #                 fileobj.close()
    #             except Exception:
    #                 pass
    #         if resp_body_f:
    #             try:
    #                 resp_body_f.flush()
    #                 resp_body_f.close()
    #             except Exception:
    #                 pass

    #         # write response metadata (using only captured_request and captured_response_static)
    #         try:
    #             meta_out = {
    #                 "id": req_id,
    #                 "timestamp": time.strftime("%Y%m%dT%H%M%S"),
    #                 "request": {
    #                     "method": captured_request["method"],
    #                     "path": captured_request["full_path"],
    #                     "headers": captured_request["headers"],
    #                     "body_file": captured_request["body_file"],
    #                 },
    #                 "response": {
    #                     "status_code": captured_response_static["status_code"],
    #                     "reason": captured_response_static["reason"],
    #                     "headers": captured_response_static["headers"],
    #                     "body_file": str(response_body_path) if resp_body_f else None,
    #                     "content_length_header": captured_response_static["content_length_header"],
    #                     "content_type": captured_response_static["content_type"],
    #                     "x_amz_request_id": captured_response_static["x_amz_request_id"],
    #                     "elapsed_seconds": captured_response_static["elapsed_seconds"],
    #                 },
    #             }
    #             with open(response_meta_path, "w", encoding="utf-8") as mf:
    #                 json.dump(meta_out, mf, indent=2)
    #             logger.info("Saved response meta %s", response_meta_path)
    #         except Exception:
    #             logger.exception("Failed to write response meta")

    def generate():
        try:
            for chunk in resp.iter_content(chunk_size=RESPONSE_CHUNK_SIZE):
                if chunk:
                    if resp_body_f:
                        try:
                            resp_body_f.write(chunk)
                        except Exception:
                            logger.exception("Failed to write chunk to response body file")
                    yield chunk
        finally:
            # Close backend response and any file handles
            try:
                resp.close()
            except Exception:
                pass

            if body_kind == "fileobj":
                try:
                    fileobj.close()
                except Exception:
                    pass

            # Close and flush response body file so we can read it safely
            if resp_body_f:
                try:
                    resp_body_f.flush()
                    resp_body_f.close()
                except Exception:
                    logger.exception("Failed to finalize response body file")

            # Now write response metadata JSON
            try:
                meta_out = {
                    "id": req_id,
                    "timestamp": time.strftime("%Y%m%dT%H%M%S"),
                    "request": {
                        "method": captured_request["method"],
                        "path": captured_request["full_path"],
                        "headers": captured_request["headers"],
                        "body_file": captured_request["body_file"],
                    },
                    "response": {
                        "status_code": captured_response_static["status_code"],
                        "reason": captured_response_static["reason"],
                        "headers": captured_response_static["headers"],
                        "body_file": str(response_body_path) if resp_body_f else None,
                        "content_length_header": captured_response_static["content_length_header"],
                        "content_type": captured_response_static["content_type"],
                        "x_amz_request_id": captured_response_static["x_amz_request_id"],
                        "elapsed_seconds": captured_response_static["elapsed_seconds"],
                    },
                }

                # Write metadata file
                with open(response_meta_path, "w", encoding="utf-8") as mf:
                    json.dump(meta_out, mf, indent=2)
                logger.info("Saved response meta %s", response_meta_path)

            except Exception:
                logger.exception("Failed to write response meta")

            # --- improved, readable response body KV file ---
            try:
                body_kv_path = RESPONSE_LOG_DIR / f"{time.strftime('%Y%m%dT%H%M%S')}_{req_id}_body_kv.json"
                if resp_body_f and response_body_path.exists():
                    try:
                        with open(response_body_path, "rb") as bf:
                            body_bytes = bf.read()

                        # Try text path first
                        try:
                            text = body_bytes.decode("utf-8")
                            # Try JSON
                            try:
                                parsed_json = json.loads(text)
                                body_entry = {"type": "json", "value": parsed_json}
                            except Exception:
                                # Try to parse known S3 XML (ListAllMyBucketsResult) into concise structure
                                try:
                                    import xml.etree.ElementTree as ET
                                    root = ET.fromstring(text)
                                    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
                                    buckets = []
                                    # find Bucket elements (works even if namespace present)
                                    for b in root.findall(".//{http://s3.amazonaws.com/doc/2006-03-01/}Bucket"):
                                        name_el = b.find("{http://s3.amazonaws.com/doc/2006-03-01/}Name")
                                        date_el = b.find("{http://s3.amazonaws.com/doc/2006-03-01/}CreationDate")
                                        buckets.append({
                                            "name": name_el.text if name_el is not None else None,
                                            "creationDate": date_el.text if date_el is not None else None
                                        })
                                    if buckets:
                                        body_entry = {"type": "s3_bucket_list", "count": len(buckets), "buckets": buckets}
                                    else:
                                        # Fallback: plain text sample
                                        sample = text[:2000]
                                        body_entry = {"type": "text", "sample": sample, "truncated": len(text) > 2000}
                                except Exception:
                                    # Generic text fallback
                                    sample = text[:2000]
                                    body_entry = {"type": "text", "sample": sample, "truncated": len(text) > 2000}
                        except UnicodeDecodeError:
                            # Binary content: store base64 sample and size
                            import base64
                            b64 = base64.b64encode(body_bytes).decode("ascii")
                            body_entry = {"type": "binary", "size": len(body_bytes), "sample_base64": b64[:2000], "truncated": len(b64) > 2000}

                        # write the concise, readable JSON
                        with open(body_kv_path, "w", encoding="utf-8") as kvf:
                            json.dump(body_entry, kvf, indent=2, ensure_ascii=False)
                        logger.info("Saved readable response body JSON %s", body_kv_path)

                    except Exception:
                        logger.exception("Failed to create readable body key-value JSON")
                else:
                    logger.debug("No response body file to create key-value JSON for req %s", req_id)
            except Exception:
                logger.exception("Unexpected error while writing readable body key-value JSON")

    resp_headers = _filter_response_headers(resp.headers)
    flask_resp = Response(generate(), status=resp.status_code, headers=resp_headers)
    return flask_resp

if __name__ == "__main__":
    bind_host = os.environ.get("LISTEN_HOST", "0.0.0.0")
    bind_port = int(os.environ.get("LISTEN_PORT", 5001))
    logger.info("Starting S3 proxy (forwarding to %s) on %s:%s", MINIO_TARGET, bind_host, bind_port)
    # For production use, run under gunicorn or another WSGI server.
    app.run(host=bind_host, port=bind_port, threaded=True)
