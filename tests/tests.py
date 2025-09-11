# test.py
import os
import json
import types
from datetime import datetime, timedelta

import pytest
from moto import mock_aws
import boto3
from freezegun import freeze_time
from fastapi.testclient import TestClient

# ------------------------------------------------------------
# Helpers / Fakes
# ------------------------------------------------------------

class FakeCursor:
    def __init__(self):
        self.executed = []
        self.executemany_calls = []
        self.closed = False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, seq_of_params):
        self.executemany_calls.append((sql, list(seq_of_params)))

    def fetchall(self):
        # En tests de FastAPI devolvemos lo que inyecta FakeConn
        return getattr(self, "_rows", [])

    def close(self):
        self.closed = True

class FakeConn:
    def __init__(self, rows=None):
        self._rows = rows or []
        self.cursor_obj = FakeCursor()
        # Para FastAPI: que el cursor devuelva filas simuladas
        self.cursor_obj._rows = self._rows
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True

    # Context manager support
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


# ------------------------------------------------------------
# Tests app.py (Lambda que descarga y sube crudo a S3)
# ------------------------------------------------------------

@mock_aws
@freeze_time("2024-01-02 03:04:05")
def test_app_handler_uploads_raw_to_s3(requests_mock, monkeypatch):
    # Arrange
    # AWS creds fake para moto
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

    import importlib
    # Asegurar que se use el bucket de prueba
    os.environ["S3_BUCKET"] = "dolar-raw-test"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=os.environ["S3_BUCKET"])

    # Mock al endpoint BanRep
    raw_payload = b'[["1757509256000","3920.12"],["1757509266000","3921.55"]]'
    # Importa app.py dinámicamente para tomar su BANREP_URL
    app_mod = importlib.import_module("app")
    requests_mock.get(app_mod.BANREP_URL, content=raw_payload, status_code=200)

    # Congelar time.time() al epoch de freeze_time
    monkeypatch.setattr("time.time", lambda: 1704164645)  # 2024-01-02 03:04:05

    # Act
    result = app_mod.handler({}, {})

    # Assert
    assert result["bucket"] == os.environ["S3_BUCKET"]
    assert result["message"] == "OK"
    assert result["size_bytes"] == len(raw_payload)
    assert result["key"] == "dolar-1704164645.json"

    obj = s3.get_object(Bucket=result["bucket"], Key=result["key"])
    body = obj["Body"].read()
    assert body == raw_payload
    assert obj["ContentType"] == "application/json"


# ------------------------------------------------------------
# Tests subirDB.py (_read_env y handler)
# ------------------------------------------------------------

def test_subirdb_read_env_ok(monkeypatch):
    import importlib
    monkeypatch.setenv("MYSQL_HOST", "localhost")
    monkeypatch.setenv("MYSQL_USER", "root")
    monkeypatch.setenv("MYSQL_PASS", "secret")
    monkeypatch.setenv("MYSQL_DB", "testdb")
    monkeypatch.setenv("MYSQL_PORT", "3307")
    sub_mod = importlib.import_module("subirDB")
    host, user, passwd, dbname, port = sub_mod._read_env()
    assert (host, user, passwd, dbname, port) == ("localhost", "root", "secret", "testdb", 3307)

def test_subirdb_read_env_missing(monkeypatch):
    import importlib
    # Limpiar
    for k in ["MYSQL_HOST", "MYSQL_USER", "MYSQL_PASS", "MYSQL_DB", "DB_HOST", "DB_USER", "DB_PASS", "DB_NAME"]:
        monkeypatch.delenv(k, raising=False)
    sub_mod = importlib.import_module("subirDB")
    with pytest.raises(RuntimeError) as e:
        sub_mod._read_env()
    assert "ENV faltantes" in str(e.value)

def test_subirdb_read_env_placeholders(monkeypatch):
    import importlib
    monkeypatch.setenv("MYSQL_HOST", "${MYSQL_HOST}")
    monkeypatch.setenv("MYSQL_USER", "root")
    monkeypatch.setenv("MYSQL_PASS", "x")
    monkeypatch.setenv("MYSQL_DB", "db")
    sub_mod = importlib.import_module("subirDB")
    with pytest.raises(RuntimeError) as e:
        sub_mod._read_env()
    assert "placeholders" in str(e.value)

def test_subirdb_read_env_bad_port(monkeypatch):
    import importlib
    monkeypatch.setenv("MYSQL_HOST", "localhost")
    monkeypatch.setenv("MYSQL_USER", "root")
    monkeypatch.setenv("MYSQL_PASS", "x")
    monkeypatch.setenv("MYSQL_DB", "db")
    monkeypatch.setenv("MYSQL_PORT", "not-int")
    sub_mod = importlib.import_module("subirDB")
    with pytest.raises(RuntimeError) as e:
        sub_mod._read_env()
    assert "inválido" in str(e.value).lower()

@mock_aws
def test_subirdb_handler_happy_path(monkeypatch):
    """
    Verifica que:
    - lee el JSON de S3 (lista de pares [epoch_ms, valor])
    - convierte y ejecuta INSERTs con pymysql.fake
    - crea la tabla si no existe
    """
    import importlib

    # AWS fake
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "dolar-raw-test2"
    s3.create_bucket(Bucket=bucket)

    # Crear objeto dolar-*.json
    data = [
        ["1757509256000", "3920.00"],
        ["1757509266000", "3921.50"],
    ]
    s3.put_object(
        Bucket=bucket,
        Key="dolar-123.json",
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json",
    )

    # Evento S3
    event = {
        "Records": [
            {"s3": {"bucket": {"name": bucket}, "object": {"key": "dolar-123.json"}}},
            {"s3": {"bucket": {"name": bucket}, "object": {"key": "not-me.txt"}}},  # debe saltarse
        ]
    }

    # ENV DB
    monkeypatch.setenv("MYSQL_HOST", "localhost")
    monkeypatch.setenv("MYSQL_USER", "root")
    monkeypatch.setenv("MYSQL_PASS", "secret")
    monkeypatch.setenv("MYSQL_DB", "db")
    monkeypatch.setenv("MYSQL_PORT", "3306")

    # Fake pymysql.connect
    fake_conn = FakeConn()
    def fake_connect(**kwargs):
        return fake_conn

    # Inyectar fake connect en subirDB
    sub_mod = importlib.import_module("subirDB")
    monkeypatch.setattr(sub_mod.pymysql, "connect", fake_connect)

    # Act
    result = sub_mod.handler(event, {})

    # Assert
    assert result["files_processed"] == 1
    assert result["total_rows_inserted"] == 2
    # Verificar que se ejecutó CREATE TABLE y 1 INSERT masivo
    executed_sql = "".join(sql for sql, _ in fake_conn.cursor_obj.executed)
    assert "CREATE TABLE IF NOT EXISTS dolar" in executed_sql
    assert len(fake_conn.cursor_obj.executemany_calls) == 1
    insert_sql, rows = fake_conn.cursor_obj.executemany_calls[0]
    assert "INSERT INTO dolar (fechahora, valor)" in insert_sql
    # Filas convertidas
    assert rows == [
        ("2025-09-10 02:40:56", 3920.0),  # 1757509256000 ms => 2025-09-10 02:40:56 UTC-05? (from code uses naive local)
        ("2025-09-10 02:41:06", 3921.5),
    ]


# ------------------------------------------------------------
# Tests main.py (FastAPI)
# ------------------------------------------------------------

def test_fastapi_health():
    import importlib
    main_mod = importlib.import_module("main")
    client = TestClient(main_mod.app)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}

def test_fastapi_intervalo_bad_range():
    import importlib
    main_mod = importlib.import_module("main")
    client = TestClient(main_mod.app)
    now = datetime(2025, 1, 1, 12, 0, 0)
    payload = {"start": now.isoformat(), "end": now.isoformat()}  # end == start
    r = client.post("/api/v1/dolar/intervalo", json=payload)
    assert r.status_code == 400
    assert "`end` debe ser mayor" in r.json()["detail"]

def test_fastapi_intervalo_ok(monkeypatch):
    """
    Monkeypatch a main.get_conn() para devolver un FakeConn con filas simuladas.
    """
    import importlib
    main_mod = importlib.import_module("main")

    # Simular 3 filas en la tabla
    rows = [
        {"fechahora": datetime(2025, 1, 1, 10, 0, 0), "valor": 3900.12},
        {"fechahora": datetime(2025, 1, 1, 10, 5, 0), "valor": 3901.34},
        {"fechahora": datetime(2025, 1, 1, 10, 10, 0), "valor": 3899.99},
    ]

    def fake_get_conn():
        # Este FakeConn ignorará el SQL y devolverá estas filas
        return FakeConn(rows=rows)

    monkeypatch.setattr(main_mod, "get_conn", fake_get_conn)

    client = TestClient(main_mod.app)

    payload = {
        "start": "2025-01-01T09:59:00",
        "end":   "2025-01-01T10:11:00",
    }
    r = client.post("/api/v1/dolar/intervalo", json=payload)
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 3
    assert body["data"][0]["valor"] == rows[0]["valor"]
    # Asegura que las fechas vienen en ISO y son ordenadas asc
    fechas = [d["fechahora"] for d in body["data"]]
    assert fechas == sorted(fechas)

