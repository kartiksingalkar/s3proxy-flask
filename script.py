import requests
import json
from datetime import datetime, timezone
from flask import Flask, request, Response

# --- Configuration ---
# IMPORTANT: Change this to the IP address and API port (usually 9000) of your actual MinIO server.
MINIO_SERVER_URL = 'https://play.min.io:9000' 
LOG_FILE_NAME = 's3_proxy_requests.log'
# --- End of Configuration ---

app = Flask(__name__)

def log_request_details(req):
    """Logs detailed information about the incoming request to a text file."""
    log_entry = {
        # CORRECTED: Uses timezone-aware UTC datetime object
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'source_ip': req.remote_addr,
        'method': req.method,
        'path': req.full_path,
        'headers': dict(req.headers),
        'body_size_bytes': len(req.get_data()),
    }
    
    with open(LOG_FILE_NAME, 'a') as f:
        f.write(json.dumps(log_entry) + '\n')

# CORRECTED: Simplified and robust routing for all paths
@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD'])
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD'])
# def s3_transparent_proxy(path):
#     """
#     This is the core proxy function. It captures all requests, logs them,
#     and forwards them with the corrected Host header to the backend MinIO server.
#     """
#     log_request_details(request)

#     forward_url = f"{MINIO_SERVER_URL}/{path}"
    
#     # CORRECTED: Preserves the original Host header to fix SignatureDoesNotMatch errors
#     forward_headers = {key: value for (key, value) in request.headers}
#     # We must set the Host to the actual destination so the server knows how to route it.
#     forward_headers['Host'] = MINIO_SERVER_URL.split('//')[1]

#     # Use a session for better performance (connection pooling)
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
def s3_transparent_proxy(path):
    """
    This is the core proxy function. It captures all requests, logs them,
    and forwards them with the corrected Host header to the backend MinIO server.
    """
    log_request_details(request)

    forward_url = f"{MINIO_SERVER_URL}/{path}"
    
    # --- FIX IS HERE ---
    # Create a dictionary of headers to forward.
    # We must explicitly preserve the original 'Host' header.
    forward_headers = {key: value for (key, value) in request.headers}
    forward_headers['Host'] = MINIO_SERVER_URL.split('//')[1] # Set host to MinIO's hostname and port

    s = requests.Session()
    
    forwarded_resp = s.request(
        method=request.method,
        url=forward_url,
        params=request.args,
        headers=forward_headers, # Use the corrected headers
        data=request.get_data(),
        stream=True 
    )

    excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
    headers = [
        (name, value) for (name, value) in forwarded_resp.raw.headers.items()
        if name.lower() not in excluded_headers
    ]

    response = Response(forwarded_resp.iter_content(chunk_size=8192), status=forwarded_resp.status_code, headers=headers)
    
    return response
    
if __name__ == '__main__':
    print(f"🚀 S3 Proxy Server starting...")
    print(f"Forwarding requests to: {MINIO_SERVER_URL}")
    print(f"Logging requests to: {LOG_FILE_NAME}")
    app.run(host='0.0.0.0', port=5001)