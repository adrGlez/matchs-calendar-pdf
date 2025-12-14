// import { NextRequest } from 'next/server';
// import { redirect } from 'next/navigation';

export async function getScrapedMatches(start: string, end: string) {
  try {
    // const url = new URL("http://local_teamhost:8000/fcf/scrape");
    // url.searchParams.set("start", start); // "2025-12-14" o "2025-12-14T10:30"
    // url.searchParams.set("end", end);

    // const res = await fetch(url.toString());
    // if (!res.ok) throw new Error(`HTTP ${res.status}`);
    // return await res.json();
    return {
  "club_url": "https://www.fcf.cat/club/2526/mollet-ue-cf/2fab",
  "teams": 40,
  "partidos": 39,
  "ms": 87987,
  "resultados": [
    {
      "team": "MOLLET U.E.,CF. B",
      "datetime": "2025-12-20T09:00:00",
      "date": "2025-12-20",
      "time": "09:00",
      "local_team": "MOLLET U.E.,CF. B",
      "away_team": "GRANOLLERS, E.C. C",
      "url_team": "https://www.fcf.cat/equip/2526/2al12/mollet-ue-cf-b",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-7/segona-divisio-alevi-s12/grup-10/2al12/mollet-ue-cf-b/2al12/granollers-ec-c",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (1)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548999+2.213233",
        "pitch_url": "https://www.fcf.cat/camp/373"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-20T09:00:00",
      "date": "2025-12-20",
      "time": "09:00",
      "local_team": "MERCANTIL, C.E B",
      "away_team": "MOLLET U.E.,CF. A",
      "url_team": "https://www.fcf.cat/equip/2526/1b9/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-7/primera-divisio-benjami-s9/grup-6/1b9/mercantil-ce-b/1b9/mollet-ue-cf-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. CAN PUIGGENER",
        "address": "c/ del Puig de la Creu, 15, Sabadell",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.564461+2.103349",
        "pitch_url": "https://www.fcf.cat/camp/270"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-20T09:00:00",
      "date": "2025-12-20",
      "time": "09:00",
      "local_team": "MOLLET U.E.,CF. A",
      "away_team": "MOLLETENSE, U.D. A",
      "url_team": "https://www.fcf.cat/equip/2526/1ia/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-femení/primera-divisio-femeni-infantil/grup-1/1ia/mollet-ue-cf-a/1ia/molletense-ud-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (1)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548999+2.213233",
        "pitch_url": "https://www.fcf.cat/camp/373"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. D",
      "datetime": "2025-12-20T09:30:00",
      "date": "2025-12-20",
      "time": "09:30",
      "local_team": "LA ROCA PENYA BLANC BLAVA CLUB FUTBOL A",
      "away_team": "MOLLET U.E.,CF. D",
      "url_team": "https://www.fcf.cat/equip/2526/3al12/mollet-ue-cf-d",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-7/tercera-divisio-alevi-s12/grup-12/3al12/la-roca-penya-blanc-blava-club-futbol-a/3al12/mollet-ue-cf-d",
      "pitch": {
        "name": "CAMP DE FUTBOL CEM FERNANDO GONZÁLEZ RESINA",
        "address": "c/ Indústria, 9, Roca Del Vallès (La)",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.585679+2.322786",
        "pitch_url": "https://www.fcf.cat/camp/444"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. B",
      "datetime": "2025-12-20T09:30:00",
      "date": "2025-12-20",
      "time": "09:30",
      "local_team": "SENTMENAT, C.D. A",
      "away_team": "MOLLET U.E.,CF. B",
      "url_team": "https://www.fcf.cat/equip/2526/3b9/mollet-ue-cf-b",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-7/tercera-divisio-benjami-s9/grup-6/3b9/sentmenat-cd-a/3b9/mollet-ue-cf-b",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. CAN SORTS",
        "address": "Camí Bosc de Can Sorts, 2, Sentmenat",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.60822+2.132746",
        "pitch_url": "https://www.fcf.cat/camp/652"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. C",
      "datetime": "2025-12-20T10:00:00",
      "date": "2025-12-20",
      "time": "10:00",
      "local_team": "MOLLET U.E.,CF. C",
      "away_team": "SANT FOST , U.E. A",
      "url_team": "https://www.fcf.cat/equip/2526/2c15/mollet-ue-cf-c",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/cadet-segona-divisio-s15/grup-11/2c15/mollet-ue-cf-c/2c15/sant-fost-ue-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (2)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548963+2.214226",
        "pitch_url": "https://www.fcf.cat/camp/7001451"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. B",
      "datetime": "2025-12-20T10:15:00",
      "date": "2025-12-20",
      "time": "10:15",
      "local_team": "SANT FOST , U.E. A",
      "away_team": "MOLLET U.E.,CF. B",
      "url_team": "https://www.fcf.cat/equip/2526/2al11/mollet-ue-cf-b",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-7/segona-divisio-alevi-s11/grup-8/2al11/sant-fost-ue-a/2al11/mollet-ue-cf-b",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. DE SANT FOST DE CAMPSENTELLES",
        "address": "av. Mauri, 4, Sant Fost De Campsentelles",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.515224+2.237343",
        "pitch_url": "https://www.fcf.cat/camp/605"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-20T10:30:00",
      "date": "2025-12-20",
      "time": "10:30",
      "local_team": "MOLLET U.E.,CF. A",
      "away_team": "NATACIO TERRASSA, C A",
      "url_team": "https://www.fcf.cat/equip/2526/1b10/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-7/primera-divisio-benjami-s10/grup-8/1b10/mollet-ue-cf-a/1b10/natacio-terrassa-c-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (1)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548999+2.213233",
        "pitch_url": "https://www.fcf.cat/camp/373"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. B",
      "datetime": "2025-12-20T10:30:00",
      "date": "2025-12-20",
      "time": "10:30",
      "local_team": "MOLLET U.E.,CF. B",
      "away_team": "SANT FELIU DE CODINES, C.F. A",
      "url_team": "https://www.fcf.cat/equip/2526/3b10/mollet-ue-cf-b",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-7/tercera-divisio-benjami-s10/grup-4/3b10/mollet-ue-cf-b/3b10/sant-feliu-de-codines-cf-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (1)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548999+2.213233",
        "pitch_url": "https://www.fcf.cat/camp/373"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-20T11:00:00",
      "date": "2025-12-20",
      "time": "11:00",
      "local_team": "SABADELL F.C., C.E. A",
      "away_team": "MOLLET U.E.,CF. A",
      "url_team": "https://www.fcf.cat/equip/2526/pi14/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/preferent-infantil-s14/grup-3/pi14/sabadell-fc-ce-a/pi14/mollet-ue-cf-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. OLÍMPIA",
        "address": "c/ de l'Apúlia, 40, Sabadell",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.55637+2.091566",
        "pitch_url": "https://www.fcf.cat/camp/897"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-20T11:15:00",
      "date": "2025-12-20",
      "time": "11:15",
      "local_team": "BIGUES, C.E. A",
      "away_team": "MOLLET U.E.,CF. A",
      "url_team": "https://www.fcf.cat/equip/2526/pb7/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-7/prebenjami-s7/grup-4/pb7/bigues-ce-a/pb7/mollet-ue-cf-a",
      "pitch": {
        "name": "CAMP D´ESPORTS MPAL. DE BIGUES I RIELLS",
        "address": "av. l'Onze de Setembre, 21, Bigues I Riells",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.672262+2.229871",
        "pitch_url": "https://www.fcf.cat/camp/323"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-20T12:00:00",
      "date": "2025-12-20",
      "time": "12:00",
      "local_team": "MOLLET U.E.,CF. A",
      "away_team": "VILASSAR DALT-GIATSU, C.E. B",
      "url_team": "https://www.fcf.cat/equip/2526/1j/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/juvenil-primera-divisio/grup-3/1j/mollet-ue-cf-a/1j/vilassar-dalt-giatsu-ce-b",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (1)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548999+2.213233",
        "pitch_url": "https://www.fcf.cat/camp/373"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-20T12:00:00",
      "date": "2025-12-20",
      "time": "12:00",
      "local_team": "SANT CUGAT FUTBOL CLUB B",
      "away_team": "MOLLET U.E.,CF. A",
      "url_team": "https://www.fcf.cat/equip/2526/1al11/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-7/primera-divisio-alevi-s11/grup-4/1al11/sant-cugat-futbol-club-b/1al11/mollet-ue-cf-a",
      "pitch": {
        "name": "CAMP DE FUTBOL ZEM JAUME TUBAU",
        "address": "c/ Ventura Gassol, 2, Sant Cugat Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.478656+2.079279",
        "pitch_url": "https://www.fcf.cat/camp/926"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-20T12:00:00",
      "date": "2025-12-20",
      "time": "12:00",
      "local_team": "MOLLET U.E.,CF. A",
      "away_team": "LOURDES, U.D. A",
      "url_team": "https://www.fcf.cat/equip/2526/db/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-5/debutant/grup-1/db/mollet-ue-cf-a/db/lourdes-ud-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (2)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548963+2.214226",
        "pitch_url": "https://www.fcf.cat/camp/7001451"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-20T12:00:00",
      "date": "2025-12-20",
      "time": "12:00",
      "local_team": "MOLLET U.E.,CF. A",
      "away_team": "FUNDACIÓ TERRASSA FC 1906 A",
      "url_team": "https://www.fcf.cat/equip/2526/fb/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-femení/femeni-benjami/grup-3/fb/mollet-ue-cf-a/fb/fundacio-terrassa-fc-1906-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (1)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548999+2.213233",
        "pitch_url": "https://www.fcf.cat/camp/373"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. C",
      "datetime": "2025-12-20T14:00:00",
      "date": "2025-12-20",
      "time": "14:00",
      "local_team": "MOLLET U.E.,CF. C",
      "away_team": "Pª BLAUGRANA DE LA ROCA DEL VALLÈS A",
      "url_team": "https://www.fcf.cat/equip/2526/2j/mollet-ue-cf-c",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/juvenil-segona-divisio/grup-18/2j/mollet-ue-cf-c/2j/pa-blaugrana-de-la-roca-del-valles-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (1)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548999+2.213233",
        "pitch_url": "https://www.fcf.cat/camp/373"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. B",
      "datetime": "2025-12-20T14:00:00",
      "date": "2025-12-20",
      "time": "14:00",
      "local_team": "Pª BLAUGRANA DE LA ROCA DEL VALLÈS A",
      "away_team": "MOLLET U.E.,CF. B",
      "url_team": "https://www.fcf.cat/equip/2526/1c16/mollet-ue-cf-b",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/cadet-primera-divisio-s16/grup-4/1c16/pa-blaugrana-de-la-roca-del-valles-a/1c16/mollet-ue-cf-b",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. SANTA AGNÈS",
        "address": "av. del Solell, s/n, Roca Del Vallès (La)",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.600794+2.35114",
        "pitch_url": "https://www.fcf.cat/camp/661"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. B",
      "datetime": "2025-12-20T14:00:00",
      "date": "2025-12-20",
      "time": "14:00",
      "local_team": "MOLLET U.E.,CF. B",
      "away_team": "PLANADEU-ROUREDA UNIÓ ESPORTIVA FUTBOL A",
      "url_team": "https://www.fcf.cat/equip/2526/1i14/mollet-ue-cf-b",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/infantil-primera-divisio-s14/grup-5/1i14/mollet-ue-cf-b/1i14/planadeu-roureda-unio-esportiva-futbol-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (2)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548963+2.214226",
        "pitch_url": "https://www.fcf.cat/camp/7001451"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. B",
      "datetime": "2025-12-20T14:00:00",
      "date": "2025-12-20",
      "time": "14:00",
      "local_team": "TORELLO, C.F. A",
      "away_team": "MOLLET U.E.,CF. B",
      "url_team": "https://www.fcf.cat/equip/2526/2fc11/mollet-ue-cf-b",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-femení/segona-divisio-femeni-cadet-f11/grup-3/2fc11/torello-cf-a/2fc11/mollet-ue-cf-b",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. DE TORELLÓ",
        "address": "ptge. dels Esports, 20, Torelló",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:42.043616+2.259679",
        "pitch_url": "https://www.fcf.cat/camp/927"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-20T16:00:00",
      "date": "2025-12-20",
      "time": "16:00",
      "local_team": "MOLLET U.E.,CF. A",
      "away_team": "LA ROMANICA, C.F. A",
      "url_team": "https://www.fcf.cat/equip/2526/1c15/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/cadet-primera-divisio-s15/grup-5/1c15/mollet-ue-cf-a/1c15/la-romanica-cf-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (2)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548963+2.214226",
        "pitch_url": "https://www.fcf.cat/camp/7001451"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. B",
      "datetime": "2025-12-20T17:30:00",
      "date": "2025-12-20",
      "time": "17:30",
      "local_team": "CARDEDEU, F.C. A",
      "away_team": "MOLLET U.E.,CF. B",
      "url_team": "https://www.fcf.cat/equip/2526/1j/mollet-ue-cf-b",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/juvenil-primera-divisio/grup-4/1j/cardedeu-fc-a/1j/mollet-ue-cf-b",
      "pitch": {
        "name": "CAMP D´ESPORTS MPAL. DE CARDEDEU",
        "address": "c/ Llinars, S/N, Cardedeu",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.638897+2.364951",
        "pitch_url": "https://www.fcf.cat/camp/289"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. B",
      "datetime": "2025-12-20T17:30:00",
      "date": "2025-12-20",
      "time": "17:30",
      "local_team": "MASNOU AT. A",
      "away_team": "MOLLET U.E.,CF. B",
      "url_team": "https://www.fcf.cat/equip/2526/1c15/mollet-ue-cf-b",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/cadet-primera-divisio-s15/grup-3/1c15/masnou-at-a/1c15/mollet-ue-cf-b",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. D´OCATA",
        "address": "c/ Abat Escarré, s/n, Masnou (El)",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.486032+2.327105",
        "pitch_url": "https://www.fcf.cat/camp/523"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-20T18:00:00",
      "date": "2025-12-20",
      "time": "18:00",
      "local_team": "MOLLET U.E.,CF. A",
      "away_team": "CIRERA, U.D. A",
      "url_team": "https://www.fcf.cat/equip/2526/1jc/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-femení/primera-divisio-femeni-juvenil/grup-1/1jc/mollet-ue-cf-a/1jc/cirera-ud-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (1)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548999+2.213233",
        "pitch_url": "https://www.fcf.cat/camp/373"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-21T09:00:00",
      "date": "2025-12-21",
      "time": "09:00",
      "local_team": "MOLLET U.E.,CF. A",
      "away_team": "CAN RULL ROMULO TRONCHONI, CFU A",
      "url_team": "https://www.fcf.cat/equip/2526/pal12/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-7/preferent-alevi-s12/grup-3/pal12/mollet-ue-cf-a/pal12/can-rull-romulo-tronchoni-cfu-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (1)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548999+2.213233",
        "pitch_url": "https://www.fcf.cat/camp/373"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. C",
      "datetime": "2025-12-21T09:00:00",
      "date": "2025-12-21",
      "time": "09:00",
      "local_team": "LLIÇA DE VALL C.F. B",
      "away_team": "MOLLET U.E.,CF. C",
      "url_team": "https://www.fcf.cat/equip/2526/3al11/mollet-ue-cf-c",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-7/tercera-divisio-alevi-s11/grup-7/3al11/llica-de-vall-cf-b/3al11/mollet-ue-cf-c",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. DE LLIÇÀ DE VALL",
        "address": "pg. de l'Església, 3, Lliçà De Vall",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.588777+2.236833",
        "pitch_url": "https://www.fcf.cat/camp/478"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. C",
      "datetime": "2025-12-21T09:30:00",
      "date": "2025-12-21",
      "time": "09:30",
      "local_team": "MOLLET U.E.,CF. C",
      "away_team": "SANTA EULALIA RONÇANA, C.F. A",
      "url_team": "https://www.fcf.cat/equip/2526/3al12/mollet-ue-cf-c",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-7/tercera-divisio-alevi-s12/grup-11/3al12/mollet-ue-cf-c/3al12/santa-eulalia-roncana-cf-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (2)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548963+2.214226",
        "pitch_url": "https://www.fcf.cat/camp/7001451"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. C",
      "datetime": "2025-12-21T10:00:00",
      "date": "2025-12-21",
      "time": "10:00",
      "local_team": "LOURDES, U.D. A",
      "away_team": "MOLLET U.E.,CF. C",
      "url_team": "https://www.fcf.cat/equip/2526/2i13/mollet-ue-cf-c",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/infantil-segona-divisio-s13/grup-9/2i13/lourdes-ud-a/2i13/mollet-ue-cf-c",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. JOAN BOCANEGRA",
        "address": "pg. de la Ronda, s/n, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.554073+2.216818",
        "pitch_url": "https://www.fcf.cat/camp/279"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-21T10:00:00",
      "date": "2025-12-21",
      "time": "10:00",
      "local_team": "SANT FELIU DE CODINES, C.F. A",
      "away_team": "MOLLET U.E.,CF. A",
      "url_team": "https://www.fcf.cat/equip/2526/pb8/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-7/prebenjami-s8/grup-9/pb8/sant-feliu-de-codines-cf-a/pb8/mollet-ue-cf-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. FRANCESC GARRIGA",
        "address": "av. de les Escoles, 22, Sant Feliu De Codines",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.693657+2.166706",
        "pitch_url": "https://www.fcf.cat/camp/602"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. B",
      "datetime": "2025-12-21T10:30:00",
      "date": "2025-12-21",
      "time": "10:30",
      "local_team": "LLIÇA DE VALL C.F. B",
      "away_team": "MOLLET U.E.,CF. B",
      "url_team": "https://www.fcf.cat/equip/2526/2i13/mollet-ue-cf-b",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/infantil-segona-divisio-s13/grup-8/2i13/llica-de-vall-cf-b/2i13/mollet-ue-cf-b",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. DE LLIÇÀ DE VALL",
        "address": "pg. de l'Església, 3, Lliçà De Vall",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.588777+2.236833",
        "pitch_url": "https://www.fcf.cat/camp/478"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. C",
      "datetime": "2025-12-21T11:30:00",
      "date": "2025-12-21",
      "time": "11:30",
      "local_team": "MOLLET U.E.,CF. C",
      "away_team": "RODA DE TER, C.E. A",
      "url_team": "https://www.fcf.cat/equip/2526/3b9/mollet-ue-cf-c",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-7/tercera-divisio-benjami-s9/grup-4/3b9/mollet-ue-cf-c/3b9/roda-de-ter-ce-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (1)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548999+2.213233",
        "pitch_url": "https://www.fcf.cat/camp/373"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. B",
      "datetime": "2025-12-21T12:15:00",
      "date": "2025-12-21",
      "time": "12:15",
      "local_team": "FIGARÓ, CE A",
      "away_team": "MOLLET U.E.,CF. B",
      "url_team": "https://www.fcf.cat/equip/2526/4cat/mollet-ue-cf-b",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/quarta-catalana/grup-10/4cat/figaro-ce-a/4cat/mollet-ue-cf-b",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. DE FIGARÓ-MONTMANY",
        "address": "ctra. de Ribes, 2 (km 0,250), Figaró-Montmany",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.716289+2.272512",
        "pitch_url": "https://www.fcf.cat/camp/1448"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-21T12:30:00",
      "date": "2025-12-21",
      "time": "12:30",
      "local_team": "MOLLET U.E.,CF. A",
      "away_team": "PREMIA CLUB ESP. A",
      "url_team": "https://www.fcf.cat/equip/2526/2fc11/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-femení/segona-divisio-femeni-cadet-f11/grup-2/2fc11/mollet-ue-cf-a/2fc11/premia-club-esp-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (1)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548999+2.213233",
        "pitch_url": "https://www.fcf.cat/camp/373"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. C",
      "datetime": "2025-12-21T14:00:00",
      "date": "2025-12-21",
      "time": "14:00",
      "local_team": "MOLLET U.E.,CF. C",
      "away_team": "BARBERÀ CLUB DE FUTBOL C",
      "url_team": "https://www.fcf.cat/equip/2526/2i14/mollet-ue-cf-c",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/infantil-segona-divisio-s14/grup-16/2i14/mollet-ue-cf-c/2i14/barbera-club-de-futbol-c",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (2)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548963+2.214226",
        "pitch_url": "https://www.fcf.cat/camp/7001451"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-21T14:30:00",
      "date": "2025-12-21",
      "time": "14:30",
      "local_team": "MOLLET U.E.,CF. A",
      "away_team": "SANT ANDREU, U.E A",
      "url_team": "https://www.fcf.cat/equip/2526/pi13/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/preferent-infantil-s13/grup-2/pi13/mollet-ue-cf-a/pi13/sant-andreu-ue-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (1)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548999+2.213233",
        "pitch_url": "https://www.fcf.cat/camp/373"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. B",
      "datetime": "2025-12-21T16:00:00",
      "date": "2025-12-21",
      "time": "16:00",
      "local_team": "MOLLET U.E.,CF. B",
      "away_team": "VIC UNIÓ ESPORTIVA CLUB A",
      "url_team": "https://www.fcf.cat/equip/2526/2jc/mollet-ue-cf-b",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-femení/segona-divisio-femeni-juvenil/grup-4/2jc/mollet-ue-cf-b/2jc/vic-unio-esportiva-club-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (2)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548963+2.214226",
        "pitch_url": "https://www.fcf.cat/camp/7001451"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-21T16:15:00",
      "date": "2025-12-21",
      "time": "16:15",
      "local_team": "MOLLET U.E.,CF. A",
      "away_team": "AQUA HOTEL FUTBOL CLUB A",
      "url_team": "https://www.fcf.cat/equip/2526/1c16/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/cadet-primera-divisio-s16/grup-3/1c16/mollet-ue-cf-a/1c16/aqua-hotel-futbol-club-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (2)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548963+2.214226",
        "pitch_url": "https://www.fcf.cat/camp/7001451"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-21T17:00:00",
      "date": "2025-12-21",
      "time": "17:00",
      "local_team": "PALAMOS CLUB DE FUTBOL A",
      "away_team": "MOLLET U.E.,CF. A",
      "url_team": "https://www.fcf.cat/equip/2526/1cat/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/primera-catalana/grup-1/1cat/palamos-club-de-futbol-a/1cat/mollet-ue-cf-a",
      "pitch": {
        "name": "ESTADI MPAL. NOU PALAMÓS-COSTA BRAVA",
        "address": "ptge Ladislao Kubala S/N (oficina), Palamós",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.853487+3.121372",
        "pitch_url": "https://www.fcf.cat/camp/754"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. C",
      "datetime": "2025-12-21T18:00:00",
      "date": "2025-12-21",
      "time": "18:00",
      "local_team": "MOLLET U.E.,CF. C",
      "away_team": "VALLES, C.AT. C",
      "url_team": "https://www.fcf.cat/equip/2526/2c16/mollet-ue-cf-c",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-11/cadet-segona-divisio-s16/grup-11/2c16/mollet-ue-cf-c/2c16/valles-cat-c",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (1)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548999+2.213233",
        "pitch_url": "https://www.fcf.cat/camp/373"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "2025-12-21T20:00:00",
      "date": "2025-12-21",
      "time": "20:00",
      "local_team": "MOLLET U.E.,CF. A",
      "away_team": "CASTELLTERÇOL, C.F. A",
      "url_team": "https://www.fcf.cat/equip/2526/2f/mollet-ue-cf-a",
      "acta_url": "https://www.fcf.cat/acta/2526/futbol-femení/segona-divisio-femeni/grup-2/2f/mollet-ue-cf-a/2f/castelltercol-cf-a",
      "pitch": {
        "name": "CAMP DE FUTBOL MPAL. GERMANS GONZALVO (1)",
        "address": "av. Rivoli, 6, Mollet Del Vallès",
        "maps": "http://maps.google.com/maps?z=12&t=m&q=loc:41.548999+2.213233",
        "pitch_url": "https://www.fcf.cat/camp/373"
      },
      "isBye": false
    },
    {
      "team": "MOLLET U.E.,CF. A",
      "datetime": "",
      "date": "",
      "time": "",
      "local_team": "",
      "away_team": "",
      "url_team": "https://www.fcf.cat/equip/2526/2fab/mollet-ue-cf-a",
      "acta_url": "",
      "pitch": {
        "name": "",
        "address": "",
        "maps": "",
        "pitch_url": ""
      },
      "isBye": true
    }
  ]
}
  } catch (error) {
    console.error(error);
    return null;
  }
}


// export async function POST(request: NextRequest) {
//   const body = await request.json();
//   return Response.json({ received: body });
// }

// export async function DELETE(request: NextRequest) {
//   return Response.json({ message: 'Resource deleted' });
// }

// export async function PATCH(request: NextRequest) {
//   const body = await request.json();
//   return Response.json({ message: 'Resource updated', data: body });
// }

// export async function OPTIONS(request: NextRequest) {
//   return new Response(null, {
//     status: 204,
//     headers: {
//       Allow: 'GET, POST, PUT, PATCH, DELETE, OPTIONS',
//     },
//   });
// }
