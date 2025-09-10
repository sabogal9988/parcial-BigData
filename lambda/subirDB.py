import os, json, logging
from datetime import datetime
import boto3, pymysql

# ===== Logging =====
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
TABLE = "dolar"  # sin tilde


def _read_env():
    """
    Lee y valida env vars. Chequea placeholders solo en strings
    y convierte el puerto a int al final. Acepta alias DB_* / MYSQL_NAME.
    """
    def first(*keys, default=""):
        for k in keys:
            v = os.environ.get(k)
            if v is not None and str(v).strip():
                return str(v).strip()
        return default

    host = first("MYSQL_HOST", "DB_HOST")
    user = first("MYSQL_USER", "DB_USER")
    passwd = first("MYSQL_PASS", "DB_PASS")
    dbname = first("MYSQL_DB", "MYSQL_NAME", "DB_NAME")
    port_raw = first("MYSQL_PORT", "DB_PORT", default="3306") or "3306"

    # Log seguro (enmascara pass)
    safe_pass = "***" + passwd[-2:] if passwd else ""
    logger.info("[ENV] host=%s user=%s db=%s port=%s pass=%s",
                host, user, dbname, port_raw, safe_pass)

    # Validaciones
    missing = [k for k, v in {
        "host": host, "user": user, "passwd": passwd, "dbname": dbname
    }.items() if not v]
    if missing:
        raise RuntimeError(f"ENV faltantes: {missing}")

    # Detecta placeholders tipo ${VAR}
    placeholders = []
    for name, val in [("host", host), ("user", user), ("passwd", passwd), ("dbname", dbname), ("port", port_raw)]:
        if isinstance(val, str) and val.startswith("${") and val.endswith("}"):
            placeholders.append(name)
    if placeholders:
        raise RuntimeError(f"ENV con placeholders sin reemplazar: {placeholders}")

    try:
        port = int(port_raw)
    except ValueError:
        raise RuntimeError(f"MYSQL_PORT / DB_PORT inválido: {port_raw}")

    return host, user, passwd, dbname, port


def handler(event, context):
    try:
        host, user, passwd, dbname, port = _read_env()
        logger.info("[INIT] Conectando a MySQL host=%s user=%s db=%s port=%s", host, user, dbname, port)

        conn = pymysql.connect(
            host=host, user=user, password=passwd, database=dbname,
            port=port, autocommit=True, charset="utf8mb4"
        )
        cur = conn.cursor()

        # Crear tabla si no existe
        logger.info("[DB] Creando tabla %s si no existe…", TABLE)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
              fechahora DATETIME NOT NULL,
              valor DECIMAL(12,4) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        total = 0
        details = []
        records = event.get("Records", [])
        logger.info("[EVENT] Records recibidos: %d", len(records))

        for i, rec in enumerate(records, start=1):
            bucket = rec["s3"]["bucket"]["name"]
            key = rec["s3"]["object"]["key"]
            logger.info("[S3] (%d/%d) s3://%s/%s", i, len(records), bucket, key)

            # Evita procesar artefactos de Zappa u otros archivos
            if not (key.startswith("dolar-") and key.endswith(".json")):
                logger.info("[SKIP] Key no coincide con dolar-*.json: %s", key)
                continue

            obj = s3.get_object(Bucket=bucket, Key=key)
            size = obj.get("ContentLength")
            logger.info("[S3] Tamaño del objeto: %s bytes", size)

            data = json.loads(obj["Body"].read().decode("utf-8", "ignore"))
            if not isinstance(data, list):
                raise ValueError(f"JSON debe ser lista de listas [[epoch_ms, valor], ...]. Recibido: {type(data).__name__}")

            rows, bad = [], 0
            for idx, item in enumerate(data):
                try:
                    ts_ms, val = item  # ["1757509256000","3920"]
                    dt = datetime.fromtimestamp(int(ts_ms)/1000).strftime("%Y-%m-%d %H:%M:%S")
                    rows.append((dt, float(val)))
                except Exception as e:
                    bad += 1
                    if bad <= 5:
                        logger.warning("[PARSE] Fila inválida idx=%d item=%r err=%s", idx, item, e)

            logger.info("[PARSE] válidas=%d inválidas=%d", len(rows), bad)

            if rows:
                cur.executemany(f"INSERT INTO {TABLE} (fechahora, valor) VALUES (%s, %s);", rows)
                total += len(rows)
                details.append({"bucket": bucket, "key": key, "rows_inserted": len(rows)})
                logger.info("[DB] Insertadas %d filas desde %s", len(rows), key)
            else:
                logger.warning("[SKIP] Sin filas válidas en %s", key)

        cur.close()
        conn.close()
        logger.info("[DONE] Archivos procesados: %d | Filas insertadas: %d", len(details), total)
        return {"files_processed": len(details), "total_rows_inserted": total, "details": details}

    except Exception:
        logger.exception("[ERROR] Falla en la ejecución de la Lambda")
        raise
