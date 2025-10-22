import re, time
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse, urlencode, urlsplit, urlunsplit
import requests
from bs4 import BeautifulSoup

BASE = "https://www.fcf.cat"
CLUB_URL = "https://www.fcf.cat/club/2526/mollet-ue-cf/2fab"

DATE_RE = re.compile(r"\b(\d{2})-(\d{2})-(\d{4})\s+(\d{2}):(\d{2})\b")

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

def soup_of(url):
    r = SESSION.get(url, timeout=25, allow_redirects=True)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser"), r.url, r.text

def with_lang(url, lang):
    """Añade o cambia ?lang=es|ca en la URL."""
    u = urlsplit(url)
    q = {} if not u.query else dict(x.split("=",1) for x in u.query.split("&") if "=" in x)
    q["lang"] = lang
    return urlunsplit((u.scheme, u.netloc, u.path, urlencode(q), u.fragment))

def canonical_club_url(url):
    parts = list(urlparse(url))
    segs = [s for s in parts[2].split("/") if s]
    if len(segs) >= 3 and segs[0] == "club":
        segs[-1] = "pi14"   # temporada “normalizada”
        parts[2] = "/" + "/".join(segs)
        return urlunparse(parts)
    return url

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

    # dedup
    seen, out = set(), []
    for t in teams:
        if t["url"] in seen:
            continue
        seen.add(t["url"])
        out.append(t)
    return out

def parse_dt(text):
    m = DATE_RE.search(text)
    if not m: return None
    d, mth, y, H, M = map(int, m.groups())
    return datetime(y, mth, d, H, M)

def next_match_from_team(team_url, team_name):
    """
    Recorre el DOM linealmente (soup.descendants). Cuando ve una fecha, busca:
    - el último <a href="/equip/..."> visto antes -> 'local'
    - el primer <a href="/equip/..."> visto después -> 'visitante'
    """
    def scan(url):
        soup, _, _ = soup_of(url)
        descendants = list(soup.descendants)
        last_team_before = None
        candidatos = []

        # índice rápido para anchors con /equip/
        equip_positions = []
        for i, node in enumerate(descendants):
            if getattr(node, "name", None) == "a":
                href = node.get("href") or ""
                if "/equip/" in href:
                    equip_positions.append((i, node))

        # barrido: memoriza el último equipo visto; cuando aparece fecha, toma el siguiente equipo
        curr_last_team = None
        for i, node in enumerate(descendants):
            if getattr(node, "name", None) == "a":
                href = node.get("href") or ""
                if "/equip/" in href:
                    curr_last_team = node

            # fechas pueden ir como texto suelto o dentro de <a> / <span>
            text = None
            if isinstance(node, str):
                text = node
            elif getattr(node, "name", None) in ("a","span","p","div"):
                # texto directo del tag
                text = node.get_text(" ", strip=True)
            if not text:
                continue

            dt = parse_dt(text)
            if not dt:
                continue

            # tenemos fecha -> buscar el "siguiente equipo" a partir de i
            right_team = None
            for j, a in equip_positions:
                if j > i:
                    right_team = a
                    break

            left_team = curr_last_team  # último equipo visto

            if left_team is None or right_team is None:
                continue

            left_name = " ".join(left_team.get_text(strip=True).split())
            right_name = " ".join(right_team.get_text(strip=True).split())

            # filtra por equipo participante (normalizado)
            def norm(x): return re.sub(r"[\W_]+", "", (x or "")).lower()
            ref = norm(team_name)
            if ref not in (norm(left_name), norm(right_name)):
                # intenta con el título de la página como referencia
                h = soup.find(["h1","h2"])
                page_team = h.get_text(strip=True) if h else team_name
                if norm(page_team) not in (norm(left_name), norm(right_name)):
                    continue

            candidatos.append({
                "dt": dt,
                "local": left_name,
                "visitante": right_name
            })

        return candidatos

    # intenta tal cual, luego con ?lang=es y ?lang=ca
    all_cands = []
    for u in (team_url, with_lang(team_url, "es"), with_lang(team_url, "ca")):
        try:
            all_cands.extend(scan(u))
            if all_cands:
                break
        except requests.RequestException:
            continue

    if not all_cands:
        return None

    now = datetime.now()
    futuros = [c for c in all_cands if c["dt"] >= now]
    chosen = min(futuros, key=lambda c: c["dt"]) if futuros else min(all_cands, key=lambda c: abs((c["dt"]-now).total_seconds()))

    return {
        "equipo": team_name,
        "fecha_hora": chosen["dt"].strftime("%Y-%m-%d %H:%M"),
        "local": chosen["local"],
        "visitante": chosen["visitante"],
        "url_equipo": team_url,
    }

def main():
    equipos = list_teams(CLUB_URL)
    print(f"Equipos detectados: {len(equipos)}")

    # mientras depuramos, limita para ir viendo resultados rápido
    equipos = equipos[:12]

    resultados = []
    for e in equipos:
        time.sleep(0.7)  # evita rate limiting
        nxt = next_match_from_team(e["url"], e["nombre"])
        if nxt:
            resultados.append(nxt)
        else:
            print("SIN PARTIDOS:", e["nombre"], e["url"])  # debug útil

    resultados.sort(key=lambda x: x["fecha_hora"])
    from pprint import pprint
    pprint(resultados)

if __name__ == "__main__":
    main()
