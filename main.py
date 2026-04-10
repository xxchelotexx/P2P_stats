from fastapi import FastAPI, Query, HTTPException
from pymongo import MongoClient
from datetime import datetime, timezone, timedelta
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="Bybit P2P API",
    description="API para consultar datos de compras, ventas y usuarios P2P de Bybit",
    version="1.0.0"
)

# Zona horaria GMT-4
GMT_MINUS_4 = timezone(timedelta(hours=-4))
GMT_0 = timezone.utc

def get_db():
    """Conexión a MongoDB usando variables del .env"""
    mongo_url = os.getenv("MONGO_URL")
    db_name = os.getenv("MONGO_DB_NAME", "P2P_Data")
    if not mongo_url:
        raise HTTPException(status_code=500, detail="MONGO_URL no configurada en el .env")
    client = MongoClient(mongo_url)
    return client[db_name]

def gmt4_to_gmt0(dt_str: str) -> datetime:
    """
    Convierte un string de fecha/hora en GMT-4 a datetime UTC (GMT+0).
    Formato esperado: 'YYYY-MM-DD HH:MM:SS' o 'YYYY-MM-DDTHH:MM:SS'
    """
    try:
        dt_str = dt_str.replace("T", " ")
        dt_naive = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        # Asignamos la zona GMT-4 y convertimos a UTC
        dt_gmt4 = dt_naive.replace(tzinfo=GMT_MINUS_4)
        dt_utc = dt_gmt4.astimezone(GMT_0)
        return dt_utc
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"Formato de fecha inválido: '{dt_str}'. Use 'YYYY-MM-DD HH:MM:SS' o 'YYYY-MM-DDTHH:MM:SS'"
        )

def utc_to_gmt4(dt: datetime) -> str:
    """Convierte datetime UTC a string en GMT-4"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=GMT_0)
    dt_gmt4 = dt.astimezone(GMT_MINUS_4)
    return dt_gmt4.strftime("%Y-%m-%d %H:%M:%S GMT-4")

def format_record(record: dict) -> dict:
    """Formatea un documento MongoDB para la respuesta, convirtiendo fechas a GMT-4"""
    record.pop("_id", None)
    for key in ["hora", "executed_anterior", "ultima_actualizacion"]:
        if key in record and isinstance(record[key], datetime):
            record[key] = utc_to_gmt4(record[key])
    return record


# ─────────────────────────────────────────────────────────────────
# ENDPOINT 1: Resumen de compras y ventas en un rango de fechas
# ─────────────────────────────────────────────────────────────────
@app.get(
    "/resumen",
    summary="Resumen de compras y ventas por rango de fechas",
    tags=["Resumen"]
)
def resumen_compras_ventas(
    fecha_inicio: str = Query(..., examples=["2026-04-01 00:00:00"], description="Fecha/hora inicio en GMT-4 (YYYY-MM-DD HH:MM:SS)"),
    fecha_fin: str = Query(..., examples=["2026-04-07 23:59:59"], description="Fecha/hora fin en GMT-4 (YYYY-MM-DD HH:MM:SS)")
):
    """
    Retorna la **suma de montos** y **cantidad de registros** de compras y ventas
    dentro del rango de fechas indicado.

    - Las fechas de entrada deben estar en **GMT-4**.
    - Las fechas de respuesta se devuelven en **GMT-4**.
    """
    inicio_utc = gmt4_to_gmt0(fecha_inicio)
    fin_utc = gmt4_to_gmt0(fecha_fin)

    db = get_db()
    filtro = {"hora": {"$gte": inicio_utc, "$lte": fin_utc}}

    # Compras
    compras = list(db["Compras_Bybit_P2P"].find(filtro))
    total_compras = sum(r.get("monto", 0) for r in compras)

    # Ventas
    ventas = list(db["Ventas_Bybit_P2P"].find(filtro))
    total_ventas = sum(r.get("monto", 0) for r in ventas)

    return {
        "periodo": {
            "inicio": fecha_inicio + " GMT-4",
            "fin": fecha_fin + " GMT-4"
        },
        "compras": {
            "cantidad_registros": len(compras),
            "suma_monto": round(total_compras, 4)
        },
        "ventas": {
            "cantidad_registros": len(ventas),
            "suma_monto": round(total_ventas, 4)
        }
    }


# ─────────────────────────────────────────────────────────────────
# ENDPOINT 2: Todos los nombres de la tabla Names_Bybit_P2P
# ─────────────────────────────────────────────────────────────────
@app.get(
    "/nombres",
    summary="Lista de todos los nicknames registrados",
    tags=["Nombres"]
)
def obtener_nombres():
    """
    Retorna todos los **nicknames** almacenados en la colección `Names_Bybit_P2P`,
    junto con su fecha de última actualización en **GMT-4**.
    """
    db = get_db()
    registros = list(db["Names_Bybit_P2P"].find({}))

    nombres = []
    for r in registros:
        ultima = r.get("ultima_actualizacion")
        nombres.append({
            "nickname": r.get("nickname"),
        })

    return {
        "total": len(nombres),
        "nombres": nombres
    }


# ─────────────────────────────────────────────────────────────────
# ENDPOINT 3: Registros de compras/ventas por nickname y fechas
# ─────────────────────────────────────────────────────────────────
@app.get(
    "/operaciones",
    summary="Compras y ventas por nickname en un rango de fechas",
    tags=["Operaciones"]
)
def operaciones_por_nickname(
    nickname: str = Query(..., examples=["Chelitex"], description="Nickname del usuario a consultar"),
    fecha_inicio: str = Query(..., examples=["2026-04-01 00:00:00"], description="Fecha/hora inicio en GMT-4 (YYYY-MM-DD HH:MM:SS)"),
    fecha_fin: str = Query(..., examples=["2026-04-07 23:59:59"], description="Fecha/hora fin en GMT-4 (YYYY-MM-DD HH:MM:SS)")
):
    """
    Retorna todos los registros de **compras y ventas** que coincidan con el
    `nickname` indicado dentro del rango de fechas.

    - Las fechas de entrada deben estar en **GMT-4**.
    - Las fechas en la respuesta se devuelven en **GMT-4**.
    """
    inicio_utc = gmt4_to_gmt0(fecha_inicio)
    fin_utc = gmt4_to_gmt0(fecha_fin)

    db = get_db()
    filtro = {
        "nickname": nickname,
        "hora": {"$gte": inicio_utc, "$lte": fin_utc}
    }

    compras = [format_record(r) for r in db["Compras_Bybit_P2P"].find(filtro)]
    ventas = [format_record(r) for r in db["Ventas_Bybit_P2P"].find(filtro)]

    if not compras and not ventas:
        return {
            "nickname": nickname,
            "periodo": {
                "inicio": fecha_inicio + " GMT-4",
                "fin": fecha_fin + " GMT-4"
            },
            "mensaje": "No se encontraron registros para este nickname en el periodo indicado.",
            "compras": [],
            "ventas": []
        }

    return {
        "nickname": nickname,
        "periodo": {
            "inicio": fecha_inicio + " GMT-4",
            "fin": fecha_fin + " GMT-4"
        },
        "compras": {
            "cantidad": len(compras),
            "registros": compras
        },
        "ventas": {
            "cantidad": len(ventas),
            "registros": ventas
        }
    }


# ─────────────────────────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────────────────────────
@app.get("/", tags=["Health"])
def health():
    return {"status": "ok", "mensaje": "API Bybit P2P funcionando correctamente"}


# ─────────────────────────────────────────────────────────────────
# Arranque con uvicorn
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "5000"))
    reload = os.getenv("API_RELOAD", "false").lower() == "true"

    uvicorn.run("main:app", host=host, port=port, reload=reload)