import re, time, sys, logging, io, csv, json, random
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, urlunparse, urlencode, urlsplit, urlunsplit, parse_qs

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from bs4 import BeautifulSoup
from flask import Flask, request, Response, jsonify, render_template_string

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

# ---------------------------------
# WEB APP (Flask)
# ---------------------------------
app = Flask(__name__)

INDEX_HTML = """
<!doctype html>
<html lang=es>
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Mollet UE – Planilla editable & exportable</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap" rel="stylesheet">
<link href="https://unpkg.com/tabulator-tables@5.6.0/dist/css/tabulator.min.css" rel="stylesheet">
<script src="https://unpkg.com/tabulator-tables@5.6.0/dist/js/tabulator.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/jspdf@2.5.1/dist/jspdf.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/jspdf-autotable@3.8.2/dist/jspdf.plugin.autotable.min.js"></script>
<style>
  :root { --bg:#0b0c10; --card:#121318; --fg:#e5e7eb; --muted:#9ca3af; --acc:#7dd3fc; --ok:#34d399; --warn:#f59e0b; }
  * { box-sizing:border-box; }
  body { margin:0; font-family:Inter, system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans, sans-serif; background:var(--bg); color:var(--fg); }
  .wrap { max-width:1200px; margin:32px auto; padding:0 16px; }
  header { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:16px; }
  h1 { font-size: clamp(20px, 2.8vw, 32px); line-height:1.2; margin:0; }
  .card { background:linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.02)); border:1px solid rgba(255,255,255,0.08); border-radius:16px; padding:16px; box-shadow:0 8px 30px rgba(0,0,0,0.25); }
  .controls { display:flex; gap:8px; flex-wrap:wrap; }
  button, select, input, .btn { background:#1b1f2a; color:var(--fg); border:1px solid rgba(255,255,255,0.12); border-radius:10px; padding:10px 14px; font-weight:600; cursor:pointer; text-decoration:none; }
  button:hover, .btn:hover { border-color:var(--acc); }
  .hint { color: var(--muted); font-size: 14px; }
  .status { display:flex; align-items:center; gap:10px; margin-top:8px; }
  .dot { width:10px; height:10px; border-radius:50%; background:var(--warn); }
  .dot.ok { background:var(--ok); }
  footer { margin:24px 0; color:var(--muted); font-size:13px; }
  .grid { display:grid; grid-template-columns:1fr; gap:16px; }
  @media(min-width:980px){ .grid{ grid-template-columns: 1fr 330px; } }
  .aside { font-size:14px; color:var(--muted); }
  code { background:#11131a; padding:2px 6px; border-radius:6px; }
  #grid { height: 70vh; background:#0f1117; border-radius:12px; }
  .toolbar { display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>Mollet UE – Planilla semanal editable</h1>
    <div class="controls">
      <button id="run">Ejecutar scrapeo</button>
      <button id="addRow">Añadir fila</button>
      <button id="delSel">Eliminar seleccionadas</button>
      <a id="csv" class="btn" href="#" aria-disabled="true">Descargar CSV</a>
      <button id="pdf">Descargar PDF</button>
    </div>
  </header>

  <div class="grid">
    <div class="card">
      <div class="toolbar">
        <div class="dot" id="dot"></div>
        <div id="msg" class="hint">Listo para ejecutar.</div>
        <span class="hint">Fuente club: <code id="club-url"></code></span>
      </div>
      <div id="grid"></div>
    </div>

    <aside class="aside">
      <div class="card">
        <p><strong>Diseño tipo planilla</strong></p>
        <p>Las columnas replican la disposición del PDF semanal: <em>EQUIP, RIVAL, DATA, HORARI, CAMP</em>. Puedes arrastrar columnas, editar celdas, añadir/eliminar filas y luego exportar a PDF con cabeceras por <em>CATEGORIA</em>.</p>
        <p>Consejo: si el scrapeo trae <em>Local/Visitante</em>, el script intenta inferir el <em>Rival</em> y la <em>DATA/HORARI</em>; lo demás puedes completarlo a mano.</p>
        <p class="hint">Respeta los Términos de fcf.cat y limita la frecuencia de peticiones.</p>
      </div>
    </aside>
  </div>

  <footer>Editable con Tabulator · Exportable con jsPDF.</footer>
</div>
<script>
const $ = (q) => document.querySelector(q);
const dot = $('#dot');
const msg = $('#msg');
const runBtn = $('#run');
const addBtn = $('#addRow');
const delBtn = $('#delSel');
const pdfBtn = $('#pdf');
const csvLink = $('#csv');
const clubUrlEl = $('#club-url');
clubUrlEl.textContent = "" + {{ club_url_json|safe }};

function setBusy(v){ dot.classList.toggle('ok', !v); runBtn.disabled = v; }
function setMsg(t){ msg.textContent = t; }

// ---- GRID (Tabulator)
let table = new Tabulator('#grid', {
  height: '65vh',
  selectable: true,
  layout: 'fitColumns',
  columns: [
    { title: 'CATEGORIA', field: 'categoria', editor: 'input', width: 130 },
    { title: 'EQUIP', field: 'equip', editor: 'input' },
    { title: 'RIVAL', field: 'rival', editor: 'input' },
    { title: 'DATA', field: 'data', editor: 'input', width: 110 },
    { title: 'HORARI', field: 'horari', editor: 'input', width: 90 },
    { title: 'CAMP', field: 'camp', editor: 'input' },
    { title: 'URL EQUIP', field: 'url_equipo', formatter: 'link', formatterParams: { labelField: 'url_equipo', target: '_blank' }, width: 220 },
    { title: 'LOCAL', field: 'local', visible:false },
    { title: 'VISITANT', field: 'visitante', visible:false },
  ],
});

addBtn.addEventListener('click', () => table.addRow({ categoria:'', equip:'', rival:'', data:'', horari:'', camp:'' }, true));

delBtn.addEventListener('click', () => {
  const rows = table.getSelectedRows();
  rows.forEach(r => r.delete());
});

async function runScrape(){
  setBusy(true);
  setMsg('Ejecutando scrapeo...');
  const qs = new URLSearchParams();
  qs.set('days','7');
  // Puedes exponer también 'delay' si quieres controlarlo desde UI:
  // qs.set('delay','0.8');
  const res = await fetch('/api/scrape?' + qs.toString(), { method: 'POST' });
  if (!res.ok){ setMsg('Error al scrapear (HTTP '+res.status+').'); setBusy(false); return; }
  const data = await res.json();
  const rows = transform(data.resultados);
  table.setData(rows); // reemplaza
  setMsg(`Partidos: ${rows.length} · Equipos: ${data.equipos} · Tiempo: ${data.ms} ms`);
  csvLink.href = '/api/scrape.csv?' + qs.toString();
  csvLink.removeAttribute('aria-disabled');
  setBusy(false);
}

function transform(items){
  // Convierte resultados del scraper -> planilla PDF (EQUIP, RIVAL, DATA, HORARI, CAMP)
  // Heurística: si el nombre del equipo del club aparece en local, rival = visitante; si no, rival = local.
  return items.map(x => {
    const isRest = !!x.descansa;
    const dt = x.fecha_hora || '';
    const fecha = dt ? dt.slice(0,10) : '';
    const hora  = dt ? dt.slice(11,16) : '';
    const upper = (s) => (s||'').toUpperCase();
    const isMolletLocal = upper(x.local).includes('MOLLET');
    const rival = isRest ? 'DESCANSA' : (isMolletLocal ? x.visitante : x.local);
    return {
      categoria: '',
      equip: x.equipo,
      rival: rival,
      data: isRest ? '' : fecha,
      horari: isRest ? '' : hora,
      camp: '',
      url_equipo: x.url_equipo,
      local: x.local,
      visitante: x.visitante,
      descansa: isRest,
    };
  });
}

runBtn.addEventListener('click', runScrape);

// ---- Exportar PDF con agrupación por CATEGORIA
pdfBtn.addEventListener('click', async () => {
  const { jsPDF } = window.jspdf;
  const doc = new jsPDF({ orientation: 'portrait', unit: 'pt', format: 'a4' });
  const data = table.getData();
  // agrupar por categoria
  const groups = data.reduce((acc, r) => { const k = (r.categoria||'SIN CATEGORIA'); (acc[k]=acc[k]||[]).push(r); return acc; }, {});
  let first = true;
  for (const [cat, rows] of Object.entries(groups)){
    if (!first) doc.addPage();
    first = false;
    doc.setFontSize(16); doc.text(`C.F. MOLLET U.E – ${cat}`, 40, 40);
    doc.setFontSize(10); doc.text(new Date().toLocaleDateString(), 520, 40, { align: 'right' });
    const body = rows.map(r => [r.equip||'', r.rival||'', r.data||'', r.horari||'', r.camp||'']);
    doc.autoTable({
      startY: 60,
      head: [[ 'EQUIP', 'RIVAL', 'DATA', 'HORARI', 'CAMP' ]],
      body,
      styles: { fontSize: 10, cellPadding: 6 },
      headStyles: { fillColor: [240,240,240], textColor: 10 },
      theme: 'grid',
      margin: { left: 40, right: 40 },
      tableWidth: 'auto',
      columnStyles: {
        0: { cellWidth: 160 },
        1: { cellWidth: 180 },
        2: { cellWidth: 70 },
        3: { cellWidth: 60 },
        4: { cellWidth: 140 },
      }
    });
  }
  doc.save('planilla-mollet.pdf');
});
</script>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(INDEX_HTML, club_url_json=json.dumps(CLUB_URL))

@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    # delay ahora regula el throttle por petición (no se hace sleep por equipo)
    delay = float(request.args.get("delay", 0.8))
    next_days = int(request.args.get("days", 7))

    t0 = time.perf_counter()
    equipos, resultados = run_scrape(CLUB_URL, delay=delay, next_days=next_days)
    ms = int((time.perf_counter() - t0) * 1000)

    return jsonify({
        "club_url": CLUB_URL,
        "equipos": len(equipos),
        "partidos": len(resultados),
        "ms": ms,
        "resultados": resultados,
    })

@app.route("/api/scrape.csv", methods=["GET", "POST"])  # permite GET para el enlace
def api_scrape_csv():
    delay = float(request.args.get("delay", 0.8))

    # ajustar ritmo también aquí para que coincida con el scrape principal
    set_rate_from_delay(delay)
    equipos, resultados = run_scrape(CLUB_URL, delay=delay)

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Fecha/Hora", "Local", "Visitante", "Equipo", "URL equipo", "Descansa"])
    for r in resultados:
        writer.writerow([r.get("fecha_hora",""), r.get("local",""), r.get("visitante",""), r.get("equipo",""), r.get("url_equipo",""), 'SI' if r.get('descansa') else 'NO'])

    out = buf.getvalue().encode("utf-8")
    return Response(out, headers={
        "Content-Type": "text/csv; charset=utf-8",
        "Content-Disposition": "attachment; filename=mollet_proximos_partidos.csv",
    })

if __name__ == "__main__":
    # Cómo ejecutar:
    # 1) pip install flask requests beautifulsoup4
    # 2) python fcf-mollet-webapp.py
    # 3) Visita http://127.0.0.1:5000
    app.run(debug=True, host="0.0.0.0", port=5000)
