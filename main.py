# app.py
import os
from typing import List
from datetime import datetime

import pymysql
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ---------- Modelos ----------
class IntervalRequest(BaseModel):
    # Acepta "YYYY-MM-DD HH:MM:SS" o ISO "YYYY-MM-DDTHH:MM:SS"
    start: datetime
    end: datetime

class Point(BaseModel):
    fechahora: datetime
    valor: float

class IntervalResponse(BaseModel):
    count: int
    data: List[Point]

# ---------- App ----------
app = FastAPI(title="Dolar API", version="1.0.0")

def get_conn():
    try:
        return pymysql.connect(
            host=os.environ["MYSQL_HOST"],
            user=os.environ["MYSQL_USER"],
            password=os.environ["MYSQL_PASS"],
            database=os.environ["MYSQL_DB"],
            port=int(os.environ.get("MYSQL_PORT", "3306")),
            cursorclass=pymysql.cursors.DictCursor,
            charset="utf8mb4",
            autocommit=True,
            connect_timeout=10,
            read_timeout=15,
            write_timeout=15,
        )
    except KeyError as e:
        raise HTTPException(status_code=500, detail=f"Variable de entorno faltante: {e}")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/api/v1/dolar/intervalo", response_model=IntervalResponse)
def query_interval(payload: IntervalRequest):
    # Validación simple
    if payload.end <= payload.start:
        raise HTTPException(status_code=400, detail="`end` debe ser mayor que `start`.")

    # Normaliza a cadena para MySQL DATETIME
    start_str = payload.start.strftime("%Y-%m-%d %H:%M:%S")
    end_str   = payload.end.strftime("%Y-%m-%d %H:%M:%S")

    sql = """
        SELECT fechahora, valor
        FROM dolar
        WHERE fechahora >= %s AND fechahora <= %s
        ORDER BY fechahora ASC
    """

    try:
        conn = get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (start_str, end_str))
                rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando la base de datos: {e}")

    data = [{"fechahora": r["fechahora"], "valor": float(r["valor"])} for r in rows]
    return {"count": len(data), "data": data}
