from flask import Flask, request, Response
import requests

app = Flask(__name__)

TARGET_BASE = "https://play.min.io:9000"

@app.route('/', defaults={'path': ''}, methods=['GET', 'PUT', 'POST', 'DELETE', 'HEAD'])
@app.route('/<path:path>', methods=['GET', 'PUT', 'POST', 'DELETE', 'HEAD'])
def proxy(path):
    url = f"{TARGET_BASE}/{path}"
    resp = requests.request(
        method=request.method,
        url=url,
        headers={k: v for k, v in request.headers if k.lower() != 'host'},
        data=request.get_data(),
        allow_redirects=False,
        verify=False  # only if you're using self-signed MinIO certs
    )
    excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
    headers = [(k, v) for k, v in resp.raw.headers.items() if k.lower() not in excluded_headers]
    response = Response(resp.content, resp.status_code, headers)
    return response

if __name__ == "__main__":
    app.run(port=5000)
