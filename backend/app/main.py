# app/main.py
from typing import Optional
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel
from datetime import datetime
import time

# importa tu core (el de arriba)
import scrape_fcf as core

app = FastAPI(title="FCF Scraper API (reusing requests+bs4 core)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

class ScrapeResp(BaseModel):
    club_url: str
    equipos: int
    partidos: int
    ms: int
    resultados: list

def _to_matches(resultados: list[dict]):
    out = []
    for r in resultados:
        if r.get("descansa"):  # sáltalos o márcalos como 'bye'
            continue
        dt = r.get("fecha_hora") or ""
        # conviértelo a ISO si existe
        iso = ""
        if dt:
            try:
                iso = datetime.strptime(dt, "%Y-%m-%d %H:%M").isoformat()+":00"
            except Exception:
                pass
        out.append({
            "id": f"{dt}-{r.get('local','')}-{r.get('visitante','')}"[:64],
            "homeTeam": r.get("local"),
            "awayTeam": r.get("visitante"),
            "date": iso,
            "league": "FCF",   # o parsea si quieres
            "round": None,
            "venue": None,
            "status": "scheduled",
            "source": {"url": r.get("url_equipo","")}
        })
    return out


# @app.get("/health")
# def health():
#     return {"ok": True}


@app.post("/fcf/scrape", response_model=ScrapeResp)
async def scrape(
    club_url: Optional[str] = Query(None, description="URL del club; por defecto el Mollet que tienes en el core"),
    days: int = Query(7, ge=1, le=31),
    delay: float = Query(0.8, ge=0.1, le=5.0),
):
    """
    Llama a tu run_scrape() en un thread (para no bloquear).
    Devuelve el mismo JSON que tu Flask /api/scrape.
    """
    t0 = time.perf_counter()
    # el core usa su CLUB_URL si no pasas uno:
    url = club_url or core.CLUB_URL

    # run_scrape es síncrono → ejecútalo en thread
    equipos, resultados = await run_in_threadpool(core.run_scrape, url, delay, days)
    ms = int((time.perf_counter() - t0) * 1000)

    return {
        "club_url": url,
        "equipos": len(equipos),
        "partidos": len(resultados),
        "ms": ms,
        "resultados": resultados,
    }


@app.post("/fcf/matches")
async def matches(
    club_url: Optional[str] = Query(None),
    days: int = Query(7, ge=1, le=31),
    delay: float = Query(0.8, ge=0.1, le=5.0),
):
    url = club_url or core.CLUB_URL
    equipos, resultados = await run_in_threadpool(core.run_scrape, url, delay, days)
    return {"matches": _to_matches(resultados)}
