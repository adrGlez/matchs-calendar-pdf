import re, time, sys, logging, io, csv, json, random
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, urlunparse, urlencode, urlsplit, urlunsplit, parse_qs

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from bs4 import BeautifulSoup

# ---------------------------------
# CONFIG ORIGEN
# ---------------------------------
BASE = "https://www.fcf.cat"
CLUB_URL = "https://www.fcf.cat/club/2526/mollet-ue-cf/2fab"

DATE_RE = re.compile(r"\b(\d{2})-(\d{2})-(\d{4})\s+(\d{2}):(\d{2})\b")

# ---------------------------------
# RATE LIMIT GLOBAL (se ajusta desde /api)
# ---------------------------------
# Estos valores se recalculan con set_rate_from_delay(base_delay)
MIN_DELAY = 0.6
MAX_DELAY = 1.2
_last_request_ts = 0.0

def set_rate_from_delay(base_delay: float):
    """
    Ajusta la ventana de jitter alrededor de base_delay.
    p.ej. base_delay=0.8 -> ~0.6–1.0 s
    """
    global MIN_DELAY, MAX_DELAY
    base_delay = max(0.1, float(base_delay))
    MIN_DELAY = max(0.1, base_delay * 0.75)
    MAX_DELAY = base_delay * 1.25

def _throttle():
    """Aplica pausa aleatoria antes de CADA petición HTTP."""
    global _last_request_ts
    wait = random.uniform(MIN_DELAY, MAX_DELAY)
    now = time.monotonic()
    elapsed = now - _last_request_ts
    if elapsed < wait:
        time.sleep(wait - elapsed)
    _last_request_ts = time.monotonic()

# ---------------------------------
# SESIÓN + RETRIES
# ---------------------------------
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; MolletScraper/1.4)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,ca-ES;q=0.9,en;q=0.8",
        "Referer": "https://www.fcf.cat/",
        "Connection": "keep-alive",
    })
    retry = Retry(
        total=4,
        backoff_factor=1.0,  # 1s, 2s, 4s, ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = make_session()

# ---------------------------------
# IDIOMA PREFERIDO (se aprende en tiempo de ejecución)
# ---------------------------------
PREFERRED_LANG = None  # 'es' | 'ca' | None

# ---------------------------------
# HELPERS
# ---------------------------------
def soup_of(url):
    """
    Throttle por petición + manejo de 429.
    Devuelve (soup, final_url, text).
    """
    _throttle()
    r = SESSION.get(url, timeout=15, allow_redirects=True)
    # backoff adicional si 429 explícito
    if r.status_code == 429:
        time.sleep(random.uniform(3, 5))
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser"), r.url, r.text

def with_lang(url, lang):
    u = urlsplit(url)
    q = {} if not u.query else dict(x.split("=", 1) for x in u.query.split("&") if "=" in x)
    q["lang"] = lang
    return urlunsplit((u.scheme, u.netloc, u.path, urlencode(q), u.fragment))

def canonical_club_url(url):
    parts = list(urlparse(url))
    segs = [s for s in parts[2].split("/") if s]
    if len(segs) >= 3 and segs[0] == "club":
        segs[-1] = "pi14"
        parts[2] = "/" + "/".join(segs)
        return urlunparse(parts)
    return url

def normalize_whitespace(s):
    return " ".join((s or "").split())

def _extract_lang_from_url(u: str) -> str | None:
    try:
        qs = parse_qs(urlsplit(u).query)
        v = qs.get("lang")
        return v[0] if v else None
    except Exception:
        return None

# ---------------------------------
# PARSEO DE EQUIPOS
# ---------------------------------
def list_teams(club_url):
    soup, final_url, _ = soup_of(club_url)
    links = soup.select('a[href^="/equip/"], a[href*="https://www.fcf.cat/equip/"]')
    teams = []
    for a in links:
        name = " ".join(a.get_text(strip=True).split())
        href = a.get("href") or ""
        if "/equip/" in href and name:
            teams.append({"nombre": name, "url": urljoin(BASE, href)})

    # fallback si no aparecen equipos en la primera variante
    if not teams:
        alt = canonical_club_url(final_url)
        candidates = [alt]
        # probar con idioma preferido primero si existe
        if PREFERRED_LANG:
            candidates.append(with_lang(alt, PREFERRED_LANG))
        # y luego otros idiomas
        for lang in ("es", "ca"):
            if PREFERRED_LANG != lang:
                candidates.append(with_lang(alt, lang))

        seen_urls = set()
        for candidate in candidates:
            if candidate in seen_urls:
                continue
            seen_urls.add(candidate)
            soup, _, _ = soup_of(candidate)
            links = soup.select('a[href^="/equip/"], a[href*="https://www.fcf.cat/equip/"]')
            for a in links:
                name = " ".join(a.get_text(strip=True).split())
                href = a.get("href") or ""
                if "/equip/" in href and name:
                    teams.append({"nombre": name, "url": urljoin(BASE, href)})

    # deduplicar
    seen, out = set(), []
    for t in teams:
        if t["url"] not in seen:
            seen.add(t["url"])
            out.append(t)
    return out

# ---------------------------------
# PARSEO DE PARTIDOS
# ---------------------------------
def extract_next_from_team_page(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table.table_resultats tr.linia")
    candidatos = []

    for tr in rows:
        team_cells = tr.select("td.resultats-w-equip")
        if len(team_cells) != 2:
            continue
        local = normalize_whitespace(team_cells[0].get_text())
        visitante = normalize_whitespace(team_cells[1].get_text())

        mid = tr.select_one("td.resultats-w-resultat")
        if not mid:
            continue

        date_div = mid.select_one("div.bg-grey")
        if date_div:
            date_node = mid.select_one("div.bg-grey.lh-data") or date_div
            date_text = normalize_whitespace(date_node.get_text())
            grey_divs = mid.select("div.bg-grey")
            time_text = ""
            if len(grey_divs) >= 2:
                time_text = normalize_whitespace(grey_divs[1].get_text())
            if not time_text:
                m = re.search(r"\b(\d{1,2}):(\d{2})\b", mid.get_text(" ", strip=True))
                time_text = m.group(0) if m else "00:00"

            try:
                dt = datetime.strptime(f"{date_text} {time_text}", "%d-%m-%Y %H:%M")
                candidatos.append((dt, local, visitante))
            except ValueError:
                continue

    if not candidatos:
        return None

    now = datetime.now()
    futuros = [c for c in candidatos if c[0] >= now]
    elegido = min(futuros, key=lambda x: x[0]) if futuros else min(candidatos, key=lambda x: abs((x[0]-now).total_seconds()))

    return {
        "fecha_hora": elegido[0].strftime("%Y-%m-%d %H:%M"),
        "local": elegido[1],
        "visitante": elegido[2],
    }

def next_match_from_team(team_url, team_name):
    """
    Estrategia:
    1) Intentar URL tal cual (camino feliz, 1 petición).
    2) Si falla, probar con idioma preferido si no está ya presente en la URL.
    3) Si sigue fallando, probar 'es' y 'ca' (evitando duplicados).
    Al encontrar datos, fijamos/actualizamos PREFERRED_LANG si se infiere del final_url.
    """
    global PREFERRED_LANG

    tried = []
    def _enqueue(u):
        if u not in tried:
            tried.append(u)

    # 1) original
    _enqueue(team_url)

    # 2) preferido (si no está ya)
    if PREFERRED_LANG and ("lang=" not in team_url):
        _enqueue(with_lang(team_url, PREFERRED_LANG))

    # 3) otros idiomas de fallback
    for lang in ("es", "ca"):
        if PREFERRED_LANG != lang and ("lang=" not in team_url or f"lang={lang}" not in team_url):
            _enqueue(with_lang(team_url, lang))

    for u in tried:
        try:
            soup, final_url, text = soup_of(u)
            data = extract_next_from_team_page(text)
            if not data:
                continue
            # aprender idioma si aplica
            lang = _extract_lang_from_url(final_url)
            if lang:
                PREFERRED_LANG = lang
            return {
                "equipo": team_name,
                "fecha_hora": data["fecha_hora"],
                "local": data["local"],
                "visitante": data["visitante"],
                "url_equipo": final_url,
            }
        except requests.RequestException:
            continue
    return None

# ---------------------------------
# SCRAPING CORE (reutilizable desde la web)
# ---------------------------------
def run_scrape(club_url: str = CLUB_URL, delay: float = 0.8, next_days: int = 7):
    """
    Ahora 'delay' ajusta el rate global por petición (no se hace sleep por equipo).
    """
    set_rate_from_delay(delay)

    equipos = list_teams(club_url)

    resultados = []
    now = datetime.now()
    window_end = now + timedelta(days=next_days)

    for e in equipos:
        try:
            nxt = next_match_from_team(e["url"], e["nombre"])
            if nxt:
                try:
                    dt = datetime.strptime(nxt["fecha_hora"], "%Y-%m-%d %H:%M")
                except Exception:
                    dt = None
                in_window = (dt is not None) and (now <= dt <= window_end)
                if in_window:
                    resultados.append(nxt)
                else:
                    resultados.append({
                        "equipo": e["nombre"],
                        "fecha_hora": "",
                        "local": "",
                        "visitante": "",
                        "url_equipo": nxt.get("url_equipo", e["url"]),
                        "descansa": True,
                    })
            else:
                resultados.append({
                    "equipo": e["nombre"],
                    "fecha_hora": "",
                    "local": "",
                    "visitante": "",
                    "url_equipo": e["url"],
                    "descansa": True,
                })
        except Exception:
            # seguimos con el resto de equipos
            resultados.append({
                "equipo": e["nombre"],
                "fecha_hora": "",
                "local": "",
                "visitante": "",
                "url_equipo": e["url"],
                "descansa": True,
            })
            continue

    # ordenar: primero los que tienen partido (por fecha), luego los que descansan
    def sort_key(x):
        if x.get("descansa"):
            return (1, "9999-99-99 99:99")
        return (0, x.get("fecha_hora") or "9999-99-99 99:99")

    resultados.sort(key=sort_key)
    return equipos, resultados
