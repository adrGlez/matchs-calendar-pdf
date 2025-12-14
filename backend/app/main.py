from typing import Optional, Any
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel
import time

import scrape_fcf as core

app = FastAPI(title="FCF Scraper API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

class ScrapeResp(BaseModel):
    club_url: str
    teams: int
    matches: int
    ms: int
    results: list[Any]

@app.get("/fcf/scrape", response_model=ScrapeResp)
async def scrape(
    club_url: Optional[str] = Query(None, description="URL del club; por defecto el Mollet del core"),
    start: str = Query(..., description="Inicio (YYYY-MM-DD o YYYY-MM-DD HH:MM o YYYY-MM-DDTHH:MM)"),
    end: str = Query(..., description="Fin (YYYY-MM-DD o YYYY-MM-DD HH:MM o YYYY-MM-DDTHH:MM)"),
    delay: float = Query(0.8, ge=0.1, le=5.0),
):
    """
    Devuelve:
      - club_url
      - teams (número de teams encontrados)
      - matches (número de results con isBye=False)
      - ms
      - results (1 partido por team dentro de ventana, o isBye=True)
    """
    t0 = time.perf_counter()
    url = club_url or core.CLUB_URL

    teams, results = await run_in_threadpool(core.run_scrape, url, delay, start, end)
    ms = int((time.perf_counter() - t0) * 1000)

    matches = sum(1 for r in results if not r.get("isBye"))

    return {
        "club_url": url,
        "teams": len(teams),
        "matches": matches,
        "ms": ms,
        "results": results,
    }
