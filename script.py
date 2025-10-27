import re, time, sys, logging
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse, urlencode, urlsplit, urlunsplit
import requests
from bs4 import BeautifulSoup

BASE = "https://www.fcf.cat"
CLUB_URL = "https://www.fcf.cat/club/2526/mollet-ue-cf/2fab"

DATE_RE = re.compile(r"\b(\d{2})-(\d{2})-(\d{4})\s+(\d{2}):(\d{2})\b")

# -------------------------
# SESIÓN
# -------------------------
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; MolletScraper/1.3)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,ca-ES;q=0.9,en;q=0.8",
        "Referer": "https://www.fcf.cat/",
        "Connection": "keep-alive",
    })
    return s

SESSION = make_session()

# -------------------------
# HELPERS
# -------------------------
def soup_of(url):
    r = SESSION.get(url, timeout=25, allow_redirects=True)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser"), r.url, r.text

def with_lang(url, lang):
    u = urlsplit(url)
    q = {} if not u.query else dict(x.split("=",1) for x in u.query.split("&") if "=" in x)
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

# -------------------------
# PARSEO DE EQUIPOS
# -------------------------
def list_teams(club_url):
    soup, final_url, _ = soup_of(club_url)
    links = soup.select('a[href^="/equip/"], a[href*="https://www.fcf.cat/equip/"]')
    teams = []
    for a in links:
        name = " ".join(a.get_text(strip=True).split())
        href = a.get("href") or ""
        if "/equip/" in href and name:
            teams.append({"nombre": name, "url": urljoin(BASE, href)})

    if not teams:
        alt = canonical_club_url(final_url)
        for candidate in (alt, with_lang(alt, "es"), with_lang(alt, "ca")):
            soup, _, _ = soup_of(candidate)
            links = soup.select('a[href^="/equip/"], a[href*="https://www.fcf.cat/equip/"]')
            for a in links:
                name = " ".join(a.get_text(strip=True).split())
                href = a.get("href") or ""
                if "/equip/" in href and name:
                    teams.append({"nombre": name, "url": urljoin(BASE, href)})

    seen, out = set(), []
    for t in teams:
        if t["url"] not in seen:
            seen.add(t["url"])
            out.append(t)
    return out

# -------------------------
# PARSEO DE PARTIDOS
# -------------------------
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
    for u in (team_url, with_lang(team_url, "es"), with_lang(team_url, "ca")):
        try:
            soup, final_url, text = soup_of(u)
            data = extract_next_from_team_page(text)
            if not data:
                continue
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

# -------------------------
# LOGGING + SALIDA BONITA
# -------------------------
def setup_logging(verbose=True):
    level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

def print_table(rows):
    if not rows:
        print("No hay partidos que mostrar.")
        return
    headers = ["Fecha/Hora", "Local", "Visitante", "Equipo", "URL equipo"]
    cols = list(zip(*(
        [r["fecha_hora"], r["local"], r["visitante"], r["equipo"], r["url_equipo"]]
        for r in rows
    )))
    widths = [max(len(h), max(len(x) for x in col)) for h, col in zip(headers, cols)]
    line = " | ".join(h.ljust(w) for h, w in zip(headers, widths))
    sep  = "-+-".join("-"*w for w in widths)
    print(line)
    print(sep)
    for r in rows:
        vals = [r["fecha_hora"], r["local"], r["visitante"], r["equipo"], r["url_equipo"]]
        print(" | ".join(v.ljust(w) for v, w in zip(vals, widths)))

# -------------------------
# MAIN
# -------------------------
def main():
    setup_logging(verbose=True)
    logging.info(f"Descargando equipos del club: {CLUB_URL}")
    try:
        equipos = list_teams(CLUB_URL)
    except Exception as e:
        logging.exception("Fallo al listar equipos")
        return

    logging.info("Equipos detectados: %d", len(equipos))
    # equipos = equipos[:12]
    resultados = []

    for idx, e in enumerate(equipos, 1):
        logging.info("[%d/%d] Buscando próximo partido de: %s", idx, len(equipos), e["nombre"])
        try:
            time.sleep(0.7)
            nxt = next_match_from_team(e["url"], e["nombre"])
        except Exception as ex:
            logging.exception("Error con el equipo %s (%s)", e["nombre"], e["url"])
            continue

        if nxt:
            logging.info("  ✔ %s vs %s @ %s", nxt["local"], nxt["visitante"], nxt["fecha_hora"])
            resultados.append(nxt)
        else:
            logging.warning("  ✖ SIN PARTIDOS: %s (%s)", e["nombre"], e["url"])

    resultados.sort(key=lambda x: x["fecha_hora"])
    print()
    print_table(resultados)
    print()
    print(f"Partidos encontrados: {len(resultados)} / Equipos procesados: {len(equipos)}", flush=True)

if __name__ == "__main__":
    main()
# test
