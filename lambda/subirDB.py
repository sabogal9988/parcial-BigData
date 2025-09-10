import os, json, logging
from datetime import datetime
import boto3, pymysql

# =======================
# Config & Logging global
# =======================
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
TABLE = "dolar"  # sin tilde


def _read_env():
    """
    Lee y valida las variables de entorno requeridas.
    Detecta placeholders del estilo ${VAR} y enmascara el password en logs.
    """
    required = ["MYSQL_HOST", "MYSQL_USER", "MYSQL_PASS", "MYSQL_DB"]
    cfg = {k: os.environ.get(k, "").strip() for k in required}
    cfg["MYSQL_PORT"] = int(os.environ.get("MYSQL_PORT", "3306").strip() or "3306")

    missing = [k for k, v in cfg.items() if k in required and not v]
    placeholders = [k for k, v in cfg.items() if v.startswith("${") and v.endswith("}")]

    # Log seguro (no exponemos el password)
    safe = {**cfg}
    if safe.get("MYSQL_PASS"):
        safe["MYSQL_PASS"] = "***" + safe["MYSQL_PASS"][-2:]
    logger.info("[ENV] MYSQL_HOST=%s MYSQL_USER=%s MYSQL_DB=%s MYSQL_PORT=%s MYSQL_PASS=%s",
                safe.get("MYSQL_HOST"), safe.get("MYSQL_USER"),
                safe.get("MYSQL_DB"), safe.get("MYSQL_PORT"),
                safe.get("MYSQL_PASS"))

    if missing:
        raise RuntimeError(f"ENV faltantes: {missing}. Revisa zappa_settings del stage que ejecuta esta Lambda.")
    if placeholders:
        raise RuntimeError(f"ENV con placeholders sin reemplazar: {placeholders}. "
                           "Asegúrate de 'quemarlas' desde GitHub Actions antes del deploy.")

    return cfg


def handler(event, context):
    try:
        # ==========
        # Conexión DB
        # ==========
        cfg = _read_env()
        host   = cfg["MYSQL_HOST"]
        user   = cfg["MYSQL_USER"]
        passwd = cfg["MYSQL_PASS"]
        dbname = cfg["MYSQL_DB"]
        port   = cfg["MYSQL_PORT"]

        logger.info("[INIT] Conectando a MySQL host=%s user=%s db=%s port=%s", host, user, dbname, port)
        conn = pymysql.connect(
            host=host, user=user, password=passwd, database=dbname,
            port=port, autocommit=True, charset="utf8mb4"
        )
        cur = conn.cursor()

        # ========================
        # Crear tabla si no existe
        # ========================
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

        # =======================
        # Procesar cada objeto S3
        # =======================
        for i, rec in enumerate(records, start=1):
            bucket = rec["s3"]["bucket"]["name"]
            key = rec["s3"]["object"]["key"]
            logger.info("[S3] (%d/%d) Leyendo s3://%s/%s", i, len(records), bucket, key)

            obj = s3.get_object(Bucket=bucket, Key=key)
            size = obj.get("ContentLength")
            logger.info("[S3] Tamaño del objeto: %s bytes", size)

            body = obj["Body"].read()
            data = json.loads(body.decode("utf-8", "ignore"))
            if not isinstance(data, list):
                raise ValueError(f"El JSON esperado es lista de listas [[epoch_ms, valor], ...]. Tipo recibido: {type(data).__name__}")

            logger.info("[JSON] Elementos nivel 1: %s", len(data))

            # -------------------------
            # Preparar filas para insert
            # -------------------------
            rows = []
            bad_rows = 0
            for idx, item in enumerate(data):
                try:
                    # esperado: ["1757509256000", "3920"] (lista de 2 elementos)
                    ts_ms, val = item
                    dt_str = datetime.fromtimestamp(int(ts_ms)/1000).strftime("%Y-%m-%d %H:%M:%S")
                    rows.append((dt_str, float(val)))
                except Exception as e:
                    bad_rows += 1
                    # limitar cantidad de logs de error por archivo
                    if bad_rows <= 5:
                        logger.warning("[PARSE] Fila inválida idx=%d contenido=%r error=%s", idx, item, e)

            logger.info("[PARSE] Filas válidas: %d | inválidas (omitidas): %d", len(rows), bad_rows)

            # -------------
            # Inserción DB
            # -------------
            if rows:
                cur.executemany(f"INSERT INTO {TABLE} (fechahora, valor) VALUES (%s, %s);", rows)
                total += len(rows)
                details.append({"bucket": bucket, "key": key, "rows_inserted": len(rows)})
                logger.info("[DB] Insertadas %d filas desde %s", len(rows), key)
            else:
                logger.warning("[SKIP] No se insertaron filas desde %s", key)

        cur.close()
        conn.close()
        logger.info("[DONE] Archivos procesados: %d | Filas totales insertadas: %d", len(details), total)

        return {"files_processed": len(details), "total_rows_inserted": total, "details": details}

    except Exception:
        logger.exception("[ERROR] Falla en la ejecución de la Lambda")
        raise
