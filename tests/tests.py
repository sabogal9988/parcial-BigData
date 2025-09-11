import os
import json
import importlib
from datetime import datetime

import pytest
from moto import mock_aws
import boto3
from freezegun import freeze_time
from fastapi.testclient import TestClient

# ============================================================
# Fakes para DB (pymysql) usados en subirDB y main
# ============================================================

class FakeCursor:
    def __init__(self, rows=None):
        self.executed = []
        self.executemany_calls = []
        self._rows = rows or []
        self.closed = False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, seq_of_params):
        self.executemany_calls.append((sql, list(seq_of_params)))

    def fetchall(self):
        return self._rows

    def close(self):
        self.closed = True


class FakeConn:
    def __init__(self, rows=None):
        self.cursor_obj = FakeCursor(rows=rows)
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


# ============================================================
# Tests app.py (paquete: lambda.app)
# ============================================================

@mock_aws
@freeze_time("2024-01-02 03:04:05")
def test_lambda_app_handler_uploads_raw_to_s3(requests_mock, monkeypatch):
    # AWS fake
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

    s3 = boto3.client("s3", region_name="us-east-1")
    os.environ["S3_BUCKET"] = "dolar-raw-test"
    s3.create_bucket(Bucket=os.environ["S3_BUCKET"])

    app_mod = importlib.import_module("lambda.app")

    raw_payload = b'[["1757509256000","3920.12"],["1757509266000","3921.55"]]'
    requests_mock.get(app_mod.BANREP_URL, content=raw_payload, status_code=200)

    # fijar time.time()
    monkeypatch.setattr("time.time", lambda: 1704164645)

    res = app_mod.handler({}, {})
    assert res["bucket"] == os.environ["S3_BUCKET"]
    assert res["message"] == "OK"
    assert res["key"] == "dolar-1704164645.json"
    assert res["size_bytes"] == len(raw_payload)

    obj = s3.get_object(Bucket=res["bucket"], Key=res["key"])
    assert obj["Body"].read() == raw_payload
    assert obj["ContentType"] == "application/json"


# ============================================================
# Tests subirDB.py (paquete: lambda.subirDB)
# ============================================================

def test_subirdb_read_env_ok(monkeypatch):
    sub_mod = importlib.import_module("lambda.subirDB")
    monkeypatch.setenv("MYSQL_HOST", "localhost")
    monkeypatch.setenv("MYSQL_USER", "root")
    monkeypatch.setenv("MYSQL_PASS", "secret")
    monkeypatch.setenv("MYSQL_DB", "testdb")
    monkeypatch.setenv("MYSQL_PORT", "3307")
    host, user, passwd, dbname, port = sub_mod._read_env()
    assert (host, user, passwd, dbname, port) == ("localhost", "root", "secret", "testdb", 3307)

def test_subirdb_read_env_missing(monkeypatch):
    sub_mod = importlib.import_module("lambda.subirDB")
    for k in ["MYSQL_HOST","MYSQL_USER","MYSQL_PASS","MYSQL_DB","DB_HOST","DB_USER","DB_PASS","DB_NAME"]:
        monkeypatch.delenv(k, raising=False)
    with pytest.raises(RuntimeError) as e:
        sub_mod._read_env()
    assert "ENV faltantes" in str(e.value)

def test_subirdb_read_env_placeholders(monkeypatch):
    sub_mod = importlib.import_module("lambda.subirDB")
    monkeypatch.setenv("MYSQL_HOST", "${MYSQL_HOST}")
    monkeypatch.setenv("MYSQL_USER", "root")
    monkeypatch.setenv("MYSQL_PASS", "x")
    monkeypatch.setenv("MYSQL_DB", "db")
    with pytest.raises(RuntimeError) as e:
        sub_mod._read_env()
    assert "placeholders" in str(e.value)

def test_subirdb_read_env_bad_port(monkeypatch):
    sub_mod = importlib.import_module("lambda.subirDB")
    monkeypatch.setenv("MYSQL_HOST", "localhost")
    monkeypatch.setenv("MYSQL_USER", "root")
    monkeypatch.setenv("MYSQL_PASS", "x")
    monkeypatch.setenv("MYSQL_DB", "db")
    monkeypatch.setenv("MYSQL_PORT", "not-int")
    with pytest.raises(RuntimeError) as e:
        sub_mod._read_env()
    assert "inválido" in str(e.value).lower()

@mock_aws
def test_subirdb_handler_happy_path(monkeypatch):
    sub_mod = importlib.import_module("lambda.subirDB")

    # AWS fake
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "dolar-raw-test2"
    s3.create_bucket(Bucket=bucket)

    # JSON de entrada (lista de [epoch_ms, valor])
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
    event = {"Records": [
        {"s3": {"bucket": {"name": bucket}, "object": {"key": "dolar-123.json"}}},
        {"s3": {"bucket": {"name": bucket}, "object": {"key": "otro.txt"}}},
    ]}

    # ENV DB
    monkeypatch.setenv("MYSQL_HOST", "localhost")
    monkeypatch.setenv("MYSQL_USER", "root")
    monkeypatch.setenv("MYSQL_PASS", "secret")
    monkeypatch.setenv("MYSQL_DB", "db")
    monkeypatch.setenv("MYSQL_PORT", "3306")

    # Fake pymysql.connect
    fake_conn = FakeConn()
    monkeypatch.setattr(sub_mod.pymysql, "connect", lambda **kwargs: fake_conn)

    res = sub_mod.handler(event, {})

    assert res["files_processed"] == 1
    assert res["total_rows_inserted"] == 2

    # Validar que se creó tabla y se hizo INSERT masivo
    executed = "".join(sql for sql, _ in fake_conn.cursor_obj.executed)
    assert "CREATE TABLE IF NOT EXISTS dolar" in executed
    assert len(fake_conn.cursor_obj.executemany_calls) == 1

    insert_sql, rows = fake_conn.cursor_obj.executemany_calls[0]
    assert "INSERT INTO dolar (fechahora, valor)" in insert_sql

    # Calcular expectativas con la misma lógica del código (naive localtime)
    ts1 = datetime.fromtimestamp(int(data[0][0]) / 1000).strftime("%Y-%m-%d %H:%M:%S")
    ts2 = datetime.fromtimestamp(int(data[1][0]) / 1000).strftime("%Y-%m-%d %H:%M:%S")
    assert rows == [(ts1, 3920.0), (ts2, 3921.5)]


# ============================================================
# Tests main.py (raíz)
# ============================================================

def test_fastapi_health():
    main_mod = importlib.import_module("main")
    client = TestClient(main_mod.app)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}

def test_fastapi_intervalo_bad_range():
    main_mod = importlib.import_module("main")
    client = TestClient(main_mod.app)
    now = datetime(2025, 1, 1, 12, 0, 0)
    payload = {"start": now.isoformat(), "end": now.isoformat()}
    r = client.post("/api/v1/dolar/intervalo", json=payload)
    assert r.status_code == 400
    assert "`end` debe ser mayor" in r.json()["detail"]

def test_fastapi_intervalo_ok(monkeypatch):
    main_mod = importlib.import_module("main")

    # filas simuladas
    rows = [
        {"fechahora": datetime(2025, 1, 1, 10, 0, 0), "valor": 3900.12},
        {"fechahora": datetime(2025, 1, 1, 10, 5, 0), "valor": 3901.34},
        {"fechahora": datetime(2025, 1, 1, 10, 10, 0), "valor": 3899.99},
    ]
    monkeypatch.setattr(main_mod, "get_conn", lambda: FakeConn(rows=rows))

    client = TestClient(main_mod.app)
    payload = {"start": "2025-01-01T09:59:00", "end": "2025-01-01T10:11:00"}
    r = client.post("/api/v1/dolar/intervalo", json=payload)
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 3
    vals = [d["valor"] for d in body["data"]]
    assert vals == [3900.12, 3901.34, 3899.99]
    fechas = [d["fechahora"] for d in body["data"]]
    assert fechas == sorted(fechas)
