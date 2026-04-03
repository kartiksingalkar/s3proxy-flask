# S3Proxy-Flask

A lightweight **Flask** application that acts as a secure gateway/proxy for **Amazon S3**. This tool allows you to serve files from an S3 bucket via standard HTTP requests, effectively abstracting the AWS SDK complexity from the client side.

## 🚀 Features

* **S3 Object Proxying**: Serve private S3 content over HTTP.
* **Streaming Support**: Efficiently streams large files directly from S3 to the client.
* **Simplified API**: Simple URL patterns to fetch objects.
* **Environment Driven**: Easy configuration via `.env` files.

## 🛠️ Installation

1.  **Clone the repository**
    ```bash
    git clone [https://github.com/kartiksingalkar/s3proxy-flask.git](https://github.com/kartiksingalkar/s3proxy-flask.git)
    cd s3proxy-flask
    ```

2.  **Create a virtual environment**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3.  **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```

## ⚙️ Configuration

Create a `.env` file in the root directory or set the following environment variables:

```env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_aws_region
S3_BUCKET_NAME=your_bucket_name
FLASK_APP=app.py
FLASK_ENV=development
