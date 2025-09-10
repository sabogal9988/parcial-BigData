import os
import re
import json
import boto3
import pymysql
from decimal import Decimal
from datetime import datetime, timezone

# --- Configuración ---
s3 = boto3.client("s3")
TABLE = "dolar"  # nombre sin tilde

# --- Funciones auxiliares ---
def _to_decimal(s):
    """Convierte a Decimal valores tipo '3918.5' o '3,918.5'."""
    if isinstance(s, (int, float, Decimal)):
        return Decimal(str(s))
    s = str(s).replace(" ", "").replace("\xa0", "")
    s = s.replace(",", ".") if s.count(",") == 1 and "." not in s else s
    s = re.sub(r"(?<=\d)[.,](?=\d{3}(?:\D|$))", "", s)
    return Decimal(s)

def _epoch_ms_to_str(ms_str):
    """Convierte epoch (ms) a string 'YYYY-MM-DD HH:MM:SS' sin ajuste de zona."""
    ms = int(ms_str)
    dt = datetime.fromtimestamp(ms / 1000)  # naive localtime
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def _iter_rows(payload):
    """Generador de filas (fechahora_str, valor) a partir de JSON tipo [[ "epoch_ms", "valor" ], ...]."""
    def gen(root):
        if isinstance(root, list):
            if root and all(isinstance(x, list) and len(x) >= 2 for x in root):
                for pair in root:
                    yield pair[0], pair[1]
            else:
                for x in root:
                    yield from gen(x)
        elif isinstance(root, dict):
            for v in root.values():
                yield from gen(v)
    for ts_str, val in gen(payload):
        yield _epoch_ms_to_str(ts_str), _to_decimal(val)

# --- Lambda principal ---
def handler(event, context):
    MYSQL_HOST = os.environ["MYSQL_HOST"]
    MYSQL_DB   = os.environ["MYSQL_DB"]
    MYSQL_USER = os.environ["MYSQL_USER"]
    MYSQL_PASS = os.environ["MYSQL_PASS"]
    

    results = []
    total_inserted = 0

    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        key = rec["s3"]["object"]["key"]

        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()
        data = json.loads(raw.decode("utf-8", errors="ignore"))

        rows = list(_iter_rows(data))
        if not rows:
            raise ValueError(f"No se encontraron filas válidas en {bucket}/{key}")

        conn = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            database=MYSQL_DB,
            autocommit=False,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.Cursor,
        )
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {TABLE} (
                          fechahora DATETIME NOT NULL,
                          valor DECIMAL(12,4) NOT NULL
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                        """
                    )
                    cur.executemany(
                        f"INSERT INTO {TABLE} (fechahora, valor) VALUES (%s, %s);",
                        rows
                    )
            inserted = len(rows)
            total_inserted += inserted
            results.append({
                "bucket": bucket,
                "key": key,
                "rows_inserted": inserted,
                "first_fechahora": rows[0][0],
                "last_fechahora": rows[-1][0],
            })
        finally:
            conn.close()

    return {"files_processed": len(results), "total_rows_inserted": total_inserted, "details": results}
