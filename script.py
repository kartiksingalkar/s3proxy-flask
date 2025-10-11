import requests
import json
from datetime import datetime
from flask import Flask, request, Response

# --- Configuration ---
# IMPORTANT: Change this to the IP address and port of your actual MinIO server.
MINIO_SERVER_URL = 'https://play.min.io:9000' # e.g., 'http://minio-instance.local:9000'
LOG_FILE_NAME = 's3_proxy_requests.log'
# --- End of Configuration ---

app = Flask(__name__)

def log_request_details(req):
    """Logs detailed information about the incoming request to a text file."""
    log_entry = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'source_ip': req.remote_addr,
        'method': req.method,
        'path': req.full_path,
        'headers': dict(req.headers),
        'body_size_bytes': len(req.get_data()),
    }
    
    # Append the log entry as a new line in the log file
    with open(LOG_FILE_NAME, 'a') as f:
        f.write(json.dumps(log_entry) + '\n')

@app.route('/minio/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD'])
@app.route('/minio/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD'])
def s3_transparent_proxy(path):
    """
    This is the core proxy function. It captures all requests to /minio/*,
    logs them, and forwards them to the backend MinIO server.
    """
    # 1. Log the incoming request details before forwarding.
    log_request_details(request)

    # 2. Prepare the request to be forwarded to the real MinIO server.
    # We construct the destination URL and pass along the headers, data, and query params.
    forward_url = f"{MINIO_SERVER_URL}/{path}"
    
    # Use a session for better performance (connection pooling)
    s = requests.Session()
    
    # Forward the request with the exact same method, headers, and body.
    # stream=True is crucial for handling large file transfers without consuming all memory.
    forwarded_resp = s.request(
        method=request.method,
        url=forward_url,
        params=request.args,
        headers=request.headers,
        data=request.get_data(),
        stream=True 
    )

    # 3. Create a response to send back to the original client.
    # We must exclude certain headers that are handled by the WSGI server.
    excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
    headers = [
        (name, value) for (name, value) in forwarded_resp.raw.headers.items()
        if name.lower() not in excluded_headers
    ]

    # Stream the response back to the client to handle large files efficiently.
    response = Response(forwarded_resp.iter_content(chunk_size=8192), status=forwarded_resp.status_code, headers=headers)
    
    return response

if __name__ == '__main__':
    print(f"🚀 S3 Proxy Server starting...")
    print(f"Forwarding requests to: {MINIO_SERVER_URL}")
    print(f"Logging requests to: {LOG_FILE_NAME}")
    # Running on 0.0.0.0 makes it accessible from other machines on the network.
    app.run(host='0.0.0.0', port=5001)