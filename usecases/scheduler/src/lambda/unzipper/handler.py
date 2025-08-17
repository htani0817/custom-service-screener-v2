# usecases/scheduler/src/lambda/unzipper/handler.py
import os, mimetypes, zipfile, boto3
from urllib.parse import unquote_plus

s3 = boto3.client("s3")

UNZIP_PREFIX = os.getenv("UNZIP_PREFIX", "unzipped/")
DELETE_ZIP   = os.getenv("DELETE_ZIP_AFTER_UNZIP", "false").lower() == "true"
SSE          = os.getenv("S3_SSE", "AES256")  # SSE-S3

CT_OVERRIDE = {
    ".html": "text/html; charset=utf-8",
    ".css" : "text/css; charset=utf-8",
    ".js"  : "application/javascript; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}

def _content_type(key: str):
    ext = os.path.splitext(key)[1].lower()
    return CT_OVERRIDE.get(ext) or mimetypes.guess_type(key)[0] or "binary/octet-stream"

def _put_file(bucket, key, body, metadata=None):
    args = {
        "Bucket": bucket,
        "Key": key,
        "Body": body,
        "ServerSideEncryption": SSE,
        "ContentType": _content_type(key),
    }
    if metadata:
        args["Metadata"] = metadata
    s3.put_object(**args)

def _unzip_to_s3(tmp_zip_path: str, bucket: str, base_prefix: str):
    with zipfile.ZipFile(tmp_zip_path, "r") as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            data = zf.read(member)
            out_key = f"{UNZIP_PREFIX}{base_prefix}{member.filename}".replace("//", "/")
            _put_file(bucket, out_key, data)

def handler(event, _):
    for rec in event.get("Records", []):
        b = rec["s3"]["bucket"]["name"]
        k = unquote_plus(rec["s3"]["object"]["key"])

        # バケットに上がる zip だけ処理
        if not k.lower().endswith(".zip"):
            continue

        # 例: results/<name>/<ts>/output.zip → base_prefix="results/<name>/<ts>/"
        base_prefix = k.rsplit("/", 1)[0] + "/"

        # /tmp にダウンロードして展開
        tmp_zip = "/tmp/output.zip"
        s3.download_file(b, k, tmp_zip)
        _unzip_to_s3(tmp_zip, b, base_prefix)

        if DELETE_ZIP:
            s3.delete_object(Bucket=b, Key=k)

    return {"status": "ok"}
