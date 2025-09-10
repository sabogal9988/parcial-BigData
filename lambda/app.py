import os
import time
import boto3
import requests

BANREP_URL = (
    "https://totoro.banrep.gov.co/estadisticas-economicas/rest/"
    "consultaDatosService/consultaMercadoCambiario"
)

S3_BUCKET = os.environ.get("S3_BUCKET", "dolar-raw-sebastian-10347")
s3 = boto3.client("s3")


def handler(event, context):
    """Lambda handler para descargar JSON crudo del BanRep y guardarlo en S3."""
    resp = requests.get(BANREP_URL, timeout=30)
    resp.raise_for_status()
    raw_bytes = resp.content  # crudo, sin modificar

    ts = int(time.time())
    key = f"dolar-{ts}.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=raw_bytes,
        ContentType="application/json",
    )

    return {
        "bucket": S3_BUCKET,
        "key": key,
        "size_bytes": len(raw_bytes),
        "message": "OK",
    }
