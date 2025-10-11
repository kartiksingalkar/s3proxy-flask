# import requests
# import json
# from datetime import datetime, timezone
# from flask import Flask, request, Response

# # --- Configuration ---
# # IMPORTANT: Change this to the IP address and API port (usually 9000) of your actual MinIO server.
# MINIO_SERVER_URL = 'https://play.min.io:9000' 
# LOG_FILE_NAME = 's3_proxy_requests.log'
# # --- End of Configuration ---

# app = Flask(__name__)

# def log_request_details(req):
#     """Logs detailed information about the incoming request to a text file."""
#     log_entry = {
#         # CORRECTED: Uses timezone-aware UTC datetime object
#         'timestamp': datetime.now(timezone.utc).isoformat(),
#         'source_ip': req.remote_addr,
#         'method': req.method,
#         'path': req.full_path,
#         'headers': dict(req.headers),
#         'body_size_bytes': len(req.get_data()),
#     }
    
#     with open(LOG_FILE_NAME, 'a') as f:
#         f.write(json.dumps(log_entry) + '\n')

# # CORRECTED: Simplified and robust routing for all paths
# @app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD'])
# @app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD'])
# # def s3_transparent_proxy(path):
# #     """
# #     This is the core proxy function. It captures all requests, logs them,
# #     and forwards them with the corrected Host header to the backend MinIO server.
# #     """
# #     log_request_details(request)

# #     forward_url = f"{MINIO_SERVER_URL}/{path}"
    
# #     # CORRECTED: Preserves the original Host header to fix SignatureDoesNotMatch errors
# #     forward_headers = {key: value for (key, value) in request.headers}
# #     # We must set the Host to the actual destination so the server knows how to route it.
# #     forward_headers['Host'] = MINIO_SERVER_URL.split('//')[1]

# #     # Use a session for better performance (connection pooling)
# #     s = requests.Session()
    
# #     forwarded_resp = s.request(
# #         method=request.method,
# #         url=forward_url,
# #         params=request.args,
# #         headers=forward_headers, # Use the corrected headers
# #         data=request.get_data(),
# #         stream=True 
# #     )

# #     excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
# #     headers = [
# #         (name, value) for (name, value) in forwarded_resp.raw.headers.items()
# #         if name.lower() not in excluded_headers
# #     ]

# #     response = Response(forwarded_resp.iter_content(chunk_size=8192), status=forwarded_resp.status_code, headers=headers)
    
# #     return response
# def s3_transparent_proxy(path):
#     """
#     This is the core proxy function. It captures all requests, logs them,
#     and forwards them with the corrected Host header to the backend MinIO server.
#     """
#     log_request_details(request)

#     forward_url = f"{MINIO_SERVER_URL}/{path}"
    
#     # --- FIX IS HERE ---
#     # Create a dictionary of headers to forward.
#     # We must explicitly preserve the original 'Host' header.
#     forward_headers = {key: value for (key, value) in request.headers}
#     forward_headers['Host'] = MINIO_SERVER_URL.split('//')[1] # Set host to MinIO's hostname and port

#     s = requests.Session()
    
#     forwarded_resp = s.request(
#         method=request.method,
#         url=forward_url,
#         params=request.args,
#         headers=forward_headers, # Use the corrected headers
#         data=request.get_data(),
#         stream=True 
#     )

#     excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
#     headers = [
#         (name, value) for (name, value) in forwarded_resp.raw.headers.items()
#         if name.lower() not in excluded_headers
#     ]

#     response = Response(forwarded_resp.iter_content(chunk_size=8192), status=forwarded_resp.status_code, headers=headers)
    
#     return response

# if __name__ == '__main__':
#     print(f"🚀 S3 Proxy Server starting...")
#     print(f"Forwarding requests to: {MINIO_SERVER_URL}")
#     print(f"Logging requests to: {LOG_FILE_NAME}")
#     app.run(host='0.0.0.0', port=5001)



#!/usr/bin/env python3
"""
app.py - Transparent S3 proxy with logging for MinIO backend.
"""
import os
import uuid
import time
import logging
from pathlib import Path
from flask import Flask, request, Response, abort
import requests
from urllib.parse import urlparse

# ---------- CONFIG ----------
# Destination MinIO server. Must include scheme and optional port.
# Example: "http://192.168.1.200:9000"
MINIO_TARGET = os.environ.get("MINIO_TARGET", "https://play.min.io:9000")

# Where to store logs
LOG_DIR = Path(os.environ.get("S3_PROXY_LOG_DIR", "./logged_requests"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Chunk size for streaming response back to client
RESPONSE_CHUNK_SIZE = 64 * 1024

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("s3proxy")

# Flask App
app = Flask(__name__)

# Hop-by-hop headers which must not be forwarded
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
}


def _unique_id():
    return uuid.uuid4().hex


def _build_target_url(full_path):
    """Builds target URL on MINIO_TARGET preserving the full path and query string."""
    return f"{MINIO_TARGET}{full_path}"


def _filter_response_headers(resp_headers):
    """Removes hop-by-hop headers from the response."""
    headers = []
    for k, v in resp_headers.items():
        if k.lower() not in HOP_BY_HOP:
            headers.append((k, v))
    return headers


@app.route("/", defaults={'path': ''}, methods=["GET", "POST", "PUT", "DELETE", "HEAD"])
@app.route("/<path:path>", methods=["GET", "POST", "PUT", "DELETE", "HEAD"])
def proxy_minio(path):
    """
    Main transparent proxy endpoint. Logs the request, then forwards it
    with the corrected Host header to MINIO_TARGET and streams the response back.
    """
    req_id = _unique_id()
    ts = time.strftime("%Y-%m-%d %H:%M:%S")

    # --- Log the request basics ---
    meta = {
        "id": req_id,
        "timestamp": ts,
        "client_addr": request.remote_addr,
        "method": request.method,
        "full_path": request.full_path,
        "content_length": request.content_length or 0,
    }
    logger.info(f"Received request: {meta}")

    # --- Prepare headers to forward ---
    # ** THE CRITICAL FIX IS HERE **
    # The Host header must be rewritten to match the target, otherwise the signature will be invalid.
    forward_headers = {k: v for k, v in request.headers}
    target_netloc = urlparse(MINIO_TARGET).netloc
    forward_headers['Host'] = target_netloc

    # --- Build target URL ---
    target_url = _build_target_url(request.full_path)

    logger.info(
        f"Proxying request id={req_id} {request.method} {request.full_path} -> {target_url}"
    )

    # --- Perform the actual forwarded request to MinIO ---
    try:
        resp = requests.request(
            method=request.method,
            url=target_url,
            headers=forward_headers,
            data=request.get_data(),  # Reads the body from the incoming request
            stream=True,
            allow_redirects=False,
            # For local MinIO with self-signed certs, you might need verify=False
            # verify=False,
        )
    except requests.RequestException as e:
        logger.exception("Error forwarding request to backend")
        return abort(502, description=f"Backend forwarding error: {e}")

    # --- Build Flask response by streaming the backend response body ---
    def generate():
        try:
            for chunk in resp.iter_content(chunk_size=RESPONSE_CHUNK_SIZE):
                yield chunk
        finally:
            resp.close()

    resp_headers = _filter_response_headers(resp.headers)
    flask_resp = Response(generate(), status=resp.status_code, headers=resp_headers)
    return flask_resp


if __name__ == "__main__":
    bind_host = os.environ.get("LISTEN_HOST", "0.0.0.0")
    # Changed default port to 5001 to match your usage
    bind_port = int(os.environ.get("LISTEN_PORT", 5001))
    logger.info(
        f"Starting S3 proxy on {bind_host}:{bind_port} (forwarding to {MINIO_TARGET})"
    )
    app.run(host=bind_host, port=bind_port, threaded=True)