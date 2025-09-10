import os, json, logging
from datetime import datetime
import boto3, pymysql

# Logs a CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
TABLE = "dolar"  # sin tilde

def handler(event, context):
    try:
        # Credenciales desde variables de entorno (no logueamos el password)
        host = os.environ["MYSQL_HOST"]
        user = os.environ["MYSQL_USER"]
        passwd = os.environ["MYSQL_PASS"]
        dbname = os.environ["MYSQL_DB"]
        logger.info(f"[INIT] Conectando a MySQL host={host} user={user} db={dbname}")

        # Conexión simple a MySQL
        conn = pymysql.connect(
            host=host,
            user=user,
            password=passwd,
            database=dbname,
            autocommit=True,
            charset="utf8mb4",
        )
        cur = conn.cursor()

        # Crea tabla si no existe
        logger.info(f"[DB] Creando tabla {TABLE} si no existe…")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
              fechahora DATETIME NOT NULL,
              valor DECIMAL(12,4) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        total = 0
        details = []
        records = event.get("Records", [])
        logger.info(f"[EVENT] Records recibidos: {len(records)}")

        for i, rec in enumerate(records, start=1):
            bucket = rec["s3"]["bucket"]["name"]
            key = rec["s3"]["object"]["key"]
            logger.info(f"[S3] ({i}/{len(records)}) Leyendo s3://{bucket}/{key}")

            obj = s3.get_object(Bucket=bucket, Key=key)
            size = obj.get("ContentLength")
            logger.info(f"[S3] Tamaño del objeto: {size} bytes")

            body = obj["Body"].read()
            data = json.loads(body.decode("utf-8", "ignore"))
            logger.info(f"[JSON] Tipo: {type(data).__name__}, elementos nivel 1: {len(data) if isinstance(data, list) else 'N/A'}")

            # Prepara filas (fechahora en DATETIME y valor float)
            rows = []
            bad_rows = 0
            for idx, item in enumerate(data):
                try:
                    ts_ms, val = item  # lista de 2 elementos: ["epoch_ms", "valor"]
                    dt = datetime.fromtimestamp(int(ts_ms)/1000).strftime("%Y-%m-%d %H:%M:%S")
                    rows.append((dt, float(val)))
                except Exception as e:
                    bad_rows += 1
                    if bad_rows <= 5:  # no saturar logs
                        logger.warning(f"[PARSE] Fila inválida en índice {idx}: {item!r} ({e})")

            logger.info(f"[PARSE] Filas válidas: {len(rows)} | inválidas (omitidas): {bad_rows}")

            if rows:
                cur.executemany(f"INSERT INTO {TABLE} (fechahora, valor) VALUES (%s, %s);", rows)
                total += len(rows)
                details.append({"bucket": bucket, "key": key, "rows_inserted": len(rows)})
                logger.info(f"[DB] Insertadas {len(rows)} filas desde {key}")
            else:
                logger.warning(f"[SKIP] No se insertaron filas desde {key}")

        cur.close()
        conn.close()
        logger.info(f"[DONE] Archivos procesados: {len(details)} | Filas totales insertadas: {total}")

        return {"files_processed": len(details), "total_rows_inserted": total, "details": details}

    except Exception as e:
        logger.exception("[ERROR] Falla en la ejecución de la Lambda")
        # Re-lanzar para que Lambda marque el intento como fallo y puedas ver el stacktrace
        raise
