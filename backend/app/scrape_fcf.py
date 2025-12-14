# scrape_fcf.py
import re, time, random
from datetime import datetime
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

# ---------------------------------
# RATE LIMIT GLOBAL (se ajusta desde /api)
# ---------------------------------
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
        "User-Agent": "Mozilla/5.0 (compatible; MolletScraper/2.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,ca-ES;q=0.9,en;q=0.8",
        "Referer": "https://www.fcf.cat/",
        "Connection": "keep-alive",
    })
    retry = Retry(
        total=4,
        backoff_factor=1.0,
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
def soup_of(url: str):
    """
    Throttle por petición + manejo de 429.
    Devuelve (soup, final_url, text).
    """
    _throttle()
    r = SESSION.get(url, timeout=20, allow_redirects=True)
    if r.status_code == 429:
        time.sleep(random.uniform(3, 5))
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser"), r.url, r.text


def with_lang(url: str, lang: str):
    u = urlsplit(url)
    q = {} if not u.query else dict(x.split("=", 1) for x in u.query.split("&") if "=" in x)
    q["lang"] = lang
    return urlunsplit((u.scheme, u.netloc, u.path, urlencode(q), u.fragment))


def canonical_club_url(url: str):
    parts = list(urlparse(url))
    segs = [s for s in parts[2].split("/") if s]
    if len(segs) >= 3 and segs[0] == "club":
        segs[-1] = "pi14"
        parts[2] = "/" + "/".join(segs)
        return urlunparse(parts)
    return url


def normalize_whitespace(s: str):
    return " ".join((s or "").split())


def _extract_lang_from_url(u: str):
    try:
        qs = parse_qs(urlsplit(u).query)
        v = qs.get("lang")
        return v[0] if v else None
    except Exception:
        return None


def parse_user_dt(s: str, is_end: bool = False) -> datetime:
    """
    Acepta:
      - YYYY-MM-DD
      - YYYY-MM-DD HH:MM
      - YYYY-MM-DDTHH:MM
    Si viene solo fecha:
      - start -> 00:00
      - end   -> 23:59
    """
    s = (s or "").strip()
    if not s:
        raise ValueError("Empty datetime")

    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass

    d = datetime.strptime(s, "%Y-%m-%d")
    return d.replace(hour=23, minute=59) if is_end else d.replace(hour=0, minute=0)


# ---------------------------------
# PARSEO DE teamS
# ---------------------------------
def list_teams(club_url: str):
    soup, final_url, _ = soup_of(club_url)
    links = soup.select('a[href^="/equip/"], a[href*="https://www.fcf.cat/equip/"]')
    teams = []
    for a in links:
        name = normalize_whitespace(a.get_text(strip=True))
        href = a.get("href") or ""
        if "/equip/" in href and name:
            teams.append({"name": name, "url": urljoin(BASE, href)})

    if not teams:
        alt = canonical_club_url(final_url)
        candidates = [alt]
        if PREFERRED_LANG:
            candidates.append(with_lang(alt, PREFERRED_LANG))
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
                name = normalize_whitespace(a.get_text(strip=True))
                href = a.get("href") or ""
                if "/equip/" in href and name:
                    teams.append({"name": name, "url": urljoin(BASE, href)})

    # deduplicar
    seen, out = set(), []
    for t in teams:
        if t["url"] not in seen:
            seen.add(t["url"])
            out.append(t)
    return out


# ---------------------------------
# PARSEO DE PARTIDOS PROGRAMADOS EN PÁGINA DE team
# ---------------------------------
def extract_scheduled_matches_from_team_page(html: str) -> list[dict]:
    """
    Devuelve lista ordenada por dt ascendente con:
      { dt, local_team, away_team, report_url }
    SOLO partidos programados (con fecha+hora en bg-grey).
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table.table_resultats tr.linia")
    matches = []

    for tr in rows:
        team_cells = tr.select("td.resultats-w-equip")
        if len(team_cells) != 2:
            continue

        local_team = normalize_whitespace(team_cells[0].get_text())
        away_team = normalize_whitespace(team_cells[1].get_text())
        if not local_team or not away_team:
            continue

        mid = tr.select_one("td.resultats-w-resultat")
        if not mid:
            continue

        # Programados: suelen tener div.bg-grey.lh-data con la fecha (DD-MM-YYYY)
        date_node = mid.select_one("div.bg-grey.lh-data")
        if not date_node:
            # No es programado (ej. "1a PART", "FINAL", etc.) o HTML diferente
            continue

        date_text = normalize_whitespace(date_node.get_text())
        grey_divs = mid.select("div.bg-grey")
        time_text = ""
        if len(grey_divs) >= 2:
            time_text = normalize_whitespace(grey_divs[1].get_text())

        if not time_text:
            m = re.search(r"\b(\d{1,2}):(\d{2})\b", mid.get_text(" ", strip=True))
            time_text = m.group(0) if m else ""

        try:
            dt = datetime.strptime(f"{date_text} {time_text}", "%d-%m-%Y %H:%M")
        except ValueError:
            continue

        a = mid.select_one("a[href]")
        report_url = ""
        if a:
            href = a.get("href") or ""
            if "/acta/" in href:
                report_url = urljoin(BASE, href)

        matches.append({
            "dt": dt,
            "local_team": local_team,
            "away_team": away_team,
            "report_url": report_url,
        })

    matches.sort(key=lambda x: x["dt"])
    return matches


# ---------------------------------
# PARSEO DE CAMPO DESDE ACTA
# ---------------------------------
def extract_pitch_from_report(html: str) -> dict:
    """
    Devuelve:
      pitch = { name, address, maps, pitch_url }
    Si no encuentra algo, lo deja en "".
    """
    soup = BeautifulSoup(html, "html.parser")
    pitch = {"name": "", "address": "", "maps": "", "pitch_url": ""}

    # CAMBIO MÍNIMO:
    # En la acta hay múltiples table.acta-table; la del estadio es la que tiene th "Estadi"/"Estadio".
    target = None
    for t in soup.select("table.acta-table"):
        th = t.select_one("thead th")
        if not th:
            continue
        label = normalize_whitespace(th.get_text(" ", strip=True)).lower()
        if label in ("estadi", "estadio"):
            target = t
            break

    if not target:
        return pitch

    camp_a = target.select_one('a[href*="/camp/"]')
    if camp_a:
        pitch["name"] = normalize_whitespace(camp_a.get_text())
        pitch["pitch_url"] = urljoin(BASE, camp_a.get("href") or "")

    maps_a = target.select_one('a[href*="maps.google.com"], a[href*="google.com/maps"]')
    if maps_a:
        pitch["maps"] = maps_a.get("href") or ""

    # La dirección suele estar en la fila con td.uppercase dentro de esta tabla
    addr_td = target.select_one("td.uppercase")
    if addr_td:
        pitch["address"] = normalize_whitespace(addr_td.get_text())

    return pitch


# ---------------------------------
# ELEGIR 1 PARTIDO POR team EN VENTANA [start, end]
# ---------------------------------
def match_in_window_from_team(team_url: str, team_name: str, start_dt: datetime, end_dt: datetime):
    """
    Estrategia:
      1) Intentar URL tal cual
      2) Si hay PREFERRED_LANG y falta lang=, probarlo
      3) fallback es/ca
    Devuelve:
      - si hay partido en ventana: isBye=False + datetime ISO + date+time + pitch
      - si no: isBye=True
    """
    global PREFERRED_LANG

    tried = []

    def _enqueue(u):
        if u not in tried:
            tried.append(u)

    _enqueue(team_url)
    if PREFERRED_LANG and ("lang=" not in team_url):
        _enqueue(with_lang(team_url, PREFERRED_LANG))
    for lang in ("es", "ca"):
        if PREFERRED_LANG != lang and ("lang=" not in team_url or f"lang={lang}" not in team_url):
            _enqueue(with_lang(team_url, lang))

    for u in tried:
        try:
            _, final_url, text = soup_of(u)

            # aprender idioma si viene en final_url
            lang = _extract_lang_from_url(final_url)
            if lang:
                PREFERRED_LANG = lang

            matches = extract_scheduled_matches_from_team_page(text)
            if not matches:
                continue

            chosen = None
            for m in matches:
                if start_dt <= m["dt"] <= end_dt:
                    chosen = m
                    break

            if not chosen:
                return {
                    "team": team_name,
                    "datetime": "",
                    "date": "",
                    "time": "",
                    "local_team": "",
                    "away_team": "",
                    "url_team": final_url,
                    "report_url": "",
                    "pitch": {"name": "", "address": "", "maps": "", "pitch_url": ""},
                    "isBye": True,
                }

            # Campo (si hay acta)
            pitch = {"name": "", "address": "", "maps": "", "pitch_url": ""}
            report_url = chosen.get("report_url") or ""
            if report_url:
                try:
                    _, _, acta_html = soup_of(report_url)
                    pitch = extract_pitch_from_report(acta_html)
                except Exception:
                    pass

            dt = chosen["dt"]
            return {
                "team": team_name,
                "datetime": dt.isoformat(timespec="seconds"),  # ISO con segundos
                "date": dt.strftime("%Y-%m-%d"),
                "time": dt.strftime("%H:%M"),
                "local_team": chosen["local_team"],
                "away_team": chosen["away_team"],
                "url_team": final_url,
                "report_url": report_url,
                "pitch": pitch,
                "isBye": False,
            }

        except requests.RequestException:
            continue

    # si todo falla
    return {
        "team": team_name,
        "datetime": "",
        "date": "",
        "time": "",
        "local_team": "",
        "away_team": "",
        "url_team": team_url,
        "report_url": "",
        "pitch": {"name": "", "address": "", "maps": "", "pitch_url": ""},
        "isBye": True,
    }


# ---------------------------------
# SCRAPING CORE (reutilizable)
# ---------------------------------
def run_scrape(club_url: str = CLUB_URL, delay: float = 0.8, start: str = "", end: str = ""):
    """
    Busca el primer partido programado por team dentro de [start, end].
    Si no hay, marca isBye=True.
    """
    set_rate_from_delay(delay)

    start_dt = parse_user_dt(start, is_end=False)
    end_dt = parse_user_dt(end, is_end=True)
    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt

    teams = list_teams(club_url)
    results = []

    for e in teams:
        try:
            r = match_in_window_from_team(e["url"], e["name"], start_dt, end_dt)
            results.append(r)
        except Exception:
            results.append({
                "team": e["name"],
                "datetime": "",
                "date": "",
                "time": "",
                "local_team": "",
                "away_team": "",
                "url_team": e["url"],
                "report_url": "",
                "pitch": {"name": "", "address": "", "maps": "", "pitch_url": ""},
                "isBye": True,
            })

    # Orden: primero con partido (por date/time), luego isBye
    def sort_key(x):
        if x.get("isBye"):
            return (1, "9999-99-99", "99:99")
        return (0, x.get("date") or "9999-99-99", x.get("time") or "99:99")

    results.sort(key=sort_key)
    return teams, results
