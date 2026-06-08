"""
ONPE 2026 - Scraper presidencial SEGUNDA VUELTA - PORTABLE (solo stdlib)
========================================================================

Version sin dependencias externas. NO requiere pip ni instalar nada.
Usa solo la biblioteca estandar de Python 3 (urllib, csv, threads).
Pensada para correr en una maquina ajena (Windows incluido) desde un USB
o carpeta compartida.

Requisito unico: que la maquina YA tenga Python 3 (>=3.7). No hay que
instalar nada. En Windows se corre con 'python' o con el lanzador 'py'.

Diferencias vs la version httpx:
    - urllib.request en vez de httpx (sin http/2; http/1.1 alcanza)
    - hilos (ThreadPoolExecutor) en vez de async
    - modulo csv en vez de pandas; sin parquet
    - solo pide gzip y lo descomprime con la stdlib (sin br/zstd)
    - headers de Chrome en Windows (coherentes con la maquina)
    - toda la salida por consola es ASCII (la consola Windows revienta
      con acentos y simbolos no-cp1252)

Uso (Windows):
    py onpe_scraper_2v_portable.py --probe-id --id-range 1,40
    py onpe_scraper_2v_portable.py --id 10 --probe
    py onpe_scraper_2v_portable.py --id 10 --out onpe_2v_out --workers 3

    (si 'py' no existe, proba con 'python')
"""

import argparse
import csv
import datetime as dt
import gzip
import json
import os
import random
import ssl
import sys
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.cookiejar import CookieJar
from urllib.parse import urlencode
from urllib.request import Request, build_opener, HTTPCookieProcessor, HTTPSHandler
from urllib.error import HTTPError, URLError

# salida UTF-8 si se puede; igual imprimimos solo ASCII por las dudas
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


# ---------------------------------------------------------------------
# Configuracion (se sobrescribe desde CLI)
# ---------------------------------------------------------------------

HOST = "resultadosegundavuelta.onpe.gob.pe"
BACKEND_PATH = "/presentacion-backend"
BASE = "https://" + HOST + BACKEND_PATH
HOME_URL = "https://" + HOST + "/main/presidenciales"
ROOT_URL = "https://" + HOST + "/"
ORIGIN = "https://" + HOST

ID_ELECCION = None
AMBITOS = {1: "peru", 2: "extranjero"}
WORKERS = 4
REQUEST_TIMEOUT = 25
MAX_RETRIES = 4
RETRY_BACKOFF = 0.8
RATE_DELAY = 0.15          # gap minimo entre requests + jitter
VERIFY_SSL = True

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/131.0.0.0 Safari/537.36")

HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Referer": HOME_URL,
    "Origin": ORIGIN,
    "DNT": "1",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}
WARMUP_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "sec-ch-ua": HEADERS["sec-ch-ua"],
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
    "Connection": "keep-alive",
}


# ---------------------------------------------------------------------
# HTTP (urllib) + limitador de ritmo entre hilos
# ---------------------------------------------------------------------

OPENER = None
_rate_lock = threading.Lock()
_next_time = [0.0]
_first_err_lock = threading.Lock()
_first_err_shown = [False]


def build_opener_global():
    global OPENER
    if VERIFY_SSL:
        ctx = ssl.create_default_context()
    else:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    OPENER = build_opener(HTTPCookieProcessor(CookieJar()), HTTPSHandler(context=ctx))


def _throttle():
    """Espacia las peticiones para no gatillar el WAF."""
    with _rate_lock:
        now = time.monotonic()
        gap = RATE_DELAY + random.random() * RATE_DELAY
        start = max(now, _next_time[0])
        _next_time[0] = start + gap
        wait = start - now
    if wait > 0:
        time.sleep(wait)


def _decode_body(resp):
    raw = resp.read()
    enc = (resp.headers.get("Content-Encoding") or "").lower()
    if enc == "gzip":
        try:
            raw = gzip.decompress(raw)
        except Exception:
            pass
    elif enc == "deflate":
        try:
            raw = zlib.decompress(raw)
        except Exception:
            try:
                raw = zlib.decompress(raw, -zlib.MAX_WBITS)
            except Exception:
                pass
    charset = "utf-8"
    ctype = resp.headers.get("Content-Type") or ""
    if "charset=" in ctype:
        charset = ctype.split("charset=")[-1].split(";")[0].strip() or "utf-8"
    return raw.decode(charset, errors="replace"), ctype


def _raw_get(url, headers):
    req = Request(url, headers=headers, method="GET")
    resp = OPENER.open(req, timeout=REQUEST_TIMEOUT)
    text, ctype = _decode_body(resp)
    return resp.getcode(), ctype, text


def warm_up():
    print("  [warm-up] visitando home de ONPE...")
    for label, url in (("root", ROOT_URL), ("presidenciales", HOME_URL)):
        try:
            code, _, text = _raw_get(url, WARMUP_HEADERS)
            print("    %s: HTTP %s (%d bytes)" % (label, code, len(text)))
        except Exception as e:
            print("    [!] warm-up %s fallo: %s" % (label, e))
    time.sleep(0.5)


def get_json(path, **params):
    """GET con throttle, reintentos y backoff. Devuelve dict parseado.

    Ante respuesta no-JSON (WAF) no re-calienta sesion: solo backoff. Imprime
    el cuerpo del primer no-JSON una vez para poder diagnosticar.
    """
    url = BASE + path
    if params:
        url += "?" + urlencode(params)
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            _throttle()
            code, ctype, text = _raw_get(url, HEADERS)
            if "json" not in ctype.lower():
                with _first_err_lock:
                    if not _first_err_shown[0]:
                        _first_err_shown[0] = True
                        snippet = text[:500]
                        print("\n  [!] primer no-JSON (HTTP %s, content-type: %s)"
                              % (code, ctype))
                        print("      url: %s" % url)
                        print("      cuerpo: %r\n" % snippet)
                raise ValueError("no-JSON HTTP %s ct=%s" % (code, ctype))
            return json.loads(text)
        except (HTTPError, URLError, ValueError, TimeoutError, OSError) as e:
            last_err = e
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF * (2 ** attempt) + random.random())
    raise last_err


# ---------------------------------------------------------------------
# Listados de ubigeos
# ---------------------------------------------------------------------

def fetch_departamentos(ambito):
    r = get_json("/ubigeos/departamentos",
                 idEleccion=ID_ELECCION, idAmbitoGeografico=ambito)
    return r["data"]


def fetch_provincias(ambito, ubigeo_dep):
    try:
        r = get_json("/ubigeos/provincias", idEleccion=ID_ELECCION,
                     idAmbitoGeografico=ambito, idUbigeoDepartamento=ubigeo_dep)
        return r["data"]
    except Exception:
        return []


def fetch_distritos(ambito, ubigeo_prov):
    try:
        r = get_json("/ubigeos/distritos", idEleccion=ID_ELECCION,
                     idAmbitoGeografico=ambito, idUbigeoProvincia=ubigeo_prov)
        return r["data"]
    except Exception:
        return []


# ---------------------------------------------------------------------
# Captura por scope
# ---------------------------------------------------------------------

def fetch_nacional():
    scope = {"nivel": "nacional", "ubigeo": None, "nombre": "PERU",
             "ubigeo_dep": None, "ubigeo_prov": None,
             "nombre_dep": None, "nombre_prov": None, "ambito": 0}
    totales = get_json("/resumen-general/totales",
                       idEleccion=ID_ELECCION, tipoFiltro="eleccion")
    cand = get_json("/eleccion-presidencial/participantes-ubicacion-geografica-nombre",
                    idEleccion=ID_ELECCION, tipoFiltro="eleccion")
    try:
        mesas = get_json("/mesa/totales", tipoFiltro="eleccion")
        mesas_data = mesas.get("data") or {}
    except Exception:
        mesas_data = {}
    return {
        "totales": dict(scope, **(totales.get("data") or {})),
        "candidatos": [dict(scope, **c) for c in (cand.get("data") or [])],
        "mesas": dict(scope, **mesas_data),
    }


def fetch_ambito_resumen(ambito):
    nombre = ("PERUANOS RESIDENTES EN EL EXTRANJERO" if ambito == 2
              else "PERU (TERRITORIO)")
    ubigeo_virtual = "800000" if ambito == 1 else "900000"
    scope = {"nivel": "ambito", "ubigeo": ubigeo_virtual, "nombre": nombre,
             "ubigeo_dep": None, "ubigeo_prov": None,
             "nombre_dep": None, "nombre_prov": None, "ambito": ambito}
    totales = get_json("/resumen-general/totales", idEleccion=ID_ELECCION,
                       tipoFiltro="ambito_geografico", idAmbitoGeografico=ambito)
    cand = get_json("/resumen-general/participantes", idEleccion=ID_ELECCION,
                    tipoFiltro="ambito_geografico", idAmbitoGeografico=ambito)
    return {
        "totales": dict(scope, **(totales.get("data") or {})),
        "candidatos": [dict(scope, **c) for c in (cand.get("data") or [])],
    }


def fetch_departamento(ambito, ub_dep, nombre_dep):
    scope = {"nivel": "departamento", "ubigeo": ub_dep, "nombre": nombre_dep,
             "ubigeo_dep": ub_dep, "ubigeo_prov": None,
             "nombre_dep": nombre_dep, "nombre_prov": None, "ambito": ambito}
    totales = get_json("/resumen-general/totales", idAmbitoGeografico=ambito,
                       idEleccion=ID_ELECCION, tipoFiltro="ubigeo_nivel_01",
                       idUbigeoDepartamento=ub_dep)
    cand = get_json("/eleccion-presidencial/participantes-ubicacion-geografica-nombre",
                    tipoFiltro="ubigeo_nivel_01", idAmbitoGeografico=ambito,
                    ubigeoNivel1=ub_dep, listRegiones="TODOS,PERU,EXTRANJERO",
                    idEleccion=ID_ELECCION)
    return {
        "totales": dict(scope, **(totales.get("data") or {})),
        "candidatos": [dict(scope, **c) for c in (cand.get("data") or [])],
    }


def fetch_provincia(ambito, ub_dep, nombre_dep, ub_prov, nombre_prov):
    scope = {"nivel": "provincia", "ubigeo": ub_prov, "nombre": nombre_prov,
             "ubigeo_dep": ub_dep, "ubigeo_prov": ub_prov,
             "nombre_dep": nombre_dep, "nombre_prov": None, "ambito": ambito}
    totales = get_json("/resumen-general/totales", idAmbitoGeografico=ambito,
                       idEleccion=ID_ELECCION, tipoFiltro="ubigeo_nivel_02",
                       idUbigeoDepartamento=ub_dep, idUbigeoProvincia=ub_prov)
    cand = get_json("/eleccion-presidencial/participantes-ubicacion-geografica-nombre",
                    tipoFiltro="ubigeo_nivel_02", idAmbitoGeografico=ambito,
                    ubigeoNivel1=ub_dep, ubigeoNivel2=ub_prov,
                    listRegiones="TODOS,PERU,EXTRANJERO", idEleccion=ID_ELECCION)
    return {
        "totales": dict(scope, **(totales.get("data") or {})),
        "candidatos": [dict(scope, **c) for c in (cand.get("data") or [])],
    }


def fetch_distrito(ambito, ub_dep, nombre_dep, ub_prov, nombre_prov,
                   ub_dist, nombre_dist):
    scope = {"nivel": "distrito", "ubigeo": ub_dist, "nombre": nombre_dist,
             "ubigeo_dep": ub_dep, "ubigeo_prov": ub_prov,
             "nombre_dep": nombre_dep, "nombre_prov": nombre_prov,
             "ambito": ambito}
    totales = get_json("/resumen-general/totales", idAmbitoGeografico=ambito,
                       idEleccion=ID_ELECCION, tipoFiltro="ubigeo_nivel_03",
                       idUbigeoDepartamento=ub_dep, idUbigeoProvincia=ub_prov,
                       idUbigeoDistrito=ub_dist)
    cand = get_json("/eleccion-presidencial/participantes-ubicacion-geografica-nombre",
                    tipoFiltro="ubigeo_nivel_03", idAmbitoGeografico=ambito,
                    ubigeoNivel1=ub_dep, ubigeoNivel2=ub_prov,
                    ubigeoNivel3=ub_dist, idEleccion=ID_ELECCION)
    return {
        "totales": dict(scope, **(totales.get("data") or {})),
        "candidatos": [dict(scope, **c) for c in (cand.get("data") or [])],
    }


# ---------------------------------------------------------------------
# Paralelizacion con hilos
# ---------------------------------------------------------------------

def run_parallel(label, fn, arg_tuples):
    results, errors = [], []
    total = len(arg_tuples)
    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fn, *args): args for args in arg_tuples}
        for fut in as_completed(futs):
            done += 1
            try:
                results.append(fut.result())
            except Exception as e:
                errors.append((futs[fut], "%s: %s" % (type(e).__name__, str(e)[:90])))
            if done % 50 == 0 or done == total:
                print("  [%s] %d/%d  (errores: %d)" % (label, done, total, len(errors)),
                      flush=True)
    if errors:
        print("  [%s] muestra de errores:" % label)
        for args, err in errors[:3]:
            print("    - %s: %s" % (str(args)[:70], err[:110]))
    return results


def capturar_ambito(ambito, incluir_distritos=True):
    label = AMBITOS[ambito]
    print("\n-- Ambito %d (%s) --" % (ambito, label))

    print("  Resumen del ambito...")
    ambito_summary = fetch_ambito_resumen(ambito)

    print("  Listando deptos...")
    deps = fetch_departamentos(ambito)
    print("    -> %d %s" % (len(deps), "departamentos" if ambito == 1 else "continentes"))
    dep_results = run_parallel(label + "/dpto", fetch_departamento,
                               [(ambito, d["ubigeo"], d["nombre"]) for d in deps])

    print("  Listando provincias...")
    provincias = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        provs_map = {ex.submit(fetch_provincias, ambito, d["ubigeo"]): d for d in deps}
        for fut in as_completed(provs_map):
            d = provs_map[fut]
            try:
                for p in fut.result():
                    provincias.append({"ubigeo_dep": d["ubigeo"], "nombre_dep": d["nombre"],
                                       "ubigeo": p["ubigeo"], "nombre": p["nombre"]})
            except Exception:
                pass
    print("    -> %d %s" % (len(provincias), "provincias" if ambito == 1 else "paises"))
    prov_results = run_parallel(
        label + "/prov", fetch_provincia,
        [(ambito, p["ubigeo_dep"], p["nombre_dep"], p["ubigeo"], p["nombre"])
         for p in provincias])

    dist_results = []
    if incluir_distritos:
        print("  Listando distritos...")
        distritos = []
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            dist_map = {ex.submit(fetch_distritos, ambito, p["ubigeo"]): p for p in provincias}
            for fut in as_completed(dist_map):
                p = dist_map[fut]
                try:
                    for d in fut.result():
                        distritos.append({
                            "ubigeo_dep": p["ubigeo_dep"], "nombre_dep": p["nombre_dep"],
                            "ubigeo_prov": p["ubigeo"], "nombre_prov": p["nombre"],
                            "ubigeo": d["ubigeo"], "nombre": d["nombre"]})
                except Exception:
                    pass
        print("    -> %d %s" % (len(distritos), "distritos" if ambito == 1 else "ciudades"))
        if distritos:
            dist_results = run_parallel(
                label + "/dist", fetch_distrito,
                [(ambito, d["ubigeo_dep"], d["nombre_dep"], d["ubigeo_prov"],
                  d["nombre_prov"], d["ubigeo"], d["nombre"]) for d in distritos])

    return {"ambito_summary": ambito_summary, "deps": dep_results,
            "provs": prov_results, "dists": dist_results,
            "n_deps": len(dep_results), "n_provs": len(prov_results),
            "n_dists": len(dist_results)}


# ---------------------------------------------------------------------
# Resumen de pendientes
# ---------------------------------------------------------------------

def _num(d, *keys):
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        try:
            return float(str(v).replace(",", ""))
        except (TypeError, ValueError):
            continue
    return None


def resumen_pendientes(capturas, top=20):
    print("\n== Pendientes por procesar ==")
    for ambito, cap in capturas.items():
        etiqueta = "DISTRITOS (Peru)" if ambito == 1 else "CIUDADES (extranjero)"
        filas = []
        for r in cap["dists"]:
            t = r["totales"]
            total = _num(t, "totalActas", "total", "totalMesas")
            cont = _num(t, "contabilizadas", "cont", "actasProcesadas")
            av = _num(t, "actasContabilizadas", "avance", "porcentajeActas")
            if total is None or cont is None:
                continue
            pend = total - cont
            if pend > 0:
                filas.append((t.get("nombre"), int(pend), int(total),
                              av if av is not None else (cont / total * 100 if total else 0)))
        filas.sort(key=lambda x: x[1], reverse=True)
        print("\n  %s: %d nodos con actas pendientes" % (etiqueta, len(filas)))
        print("  %-32s %7s %7s %9s" % ("nombre", "pend", "total", "avance%"))
        for nombre, pend, total, av in filas[:top]:
            print("  %-32s %7d %7d %9.3f" % ((nombre or "")[:32], pend, total, av))


# ---------------------------------------------------------------------
# Probe de idEleccion
# ---------------------------------------------------------------------

def probe_id(lo, hi):
    global ID_ELECCION
    print("\n> Probe idEleccion en rango [%d, %d] sobre %s" % (lo, hi, HOST))
    warm_up()
    hits = []
    for idv in range(lo, hi + 1):
        ID_ELECCION = idv
        try:
            r = get_json("/resumen-general/totales", idEleccion=idv, tipoFiltro="eleccion")
            data = r.get("data") or {}
            total = _num(data, "totalActas", "total", "totalMesas")
            cont = _num(data, "contabilizadas", "cont")
            if total and total > 0:
                hits.append(idv)
                print("  idEleccion=%3d  OK  total_actas=%d  contabilizadas=%d"
                      % (idv, int(total), int(cont or 0)))
            else:
                print("  idEleccion=%3d  .   (sin actas)" % idv)
        except Exception as e:
            print("  idEleccion=%3d  X   %s: %s" % (idv, type(e).__name__, str(e)[:50]))
        time.sleep(0.4)
    print("\n  candidatos con datos: %s" % (", ".join(str(h) for h in hits) or "ninguno"))
    print("  -> corre de nuevo con --id <ese valor> --probe para confirmar")


# ---------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------

def write_csv(path, rows):
    if not rows:
        open(path, "w", newline="", encoding="utf-8").close()
        return
    fields = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fields.append(k)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def run_snapshot(out_dir, incluir_distritos=True, ambitos_run=(1, 2), solo_probe=False):
    ts = dt.datetime.now(dt.timezone.utc)
    print("\n> %s ONPE 2V - %s" % ("PROBE" if solo_probe else "Snapshot", ts.isoformat()))
    print("  Host:    %s" % HOST)
    print("  Backend: %s" % BASE)
    print("  idEleccion: %s" % ID_ELECCION)
    print("  Ambitos: %s" % ([AMBITOS[a] for a in ambitos_run]))
    print("  Motor: urllib + hilos, workers=%d" % WORKERS)

    warm_up()

    print("\n[1/N] Nacional global...")
    nac = fetch_nacional()
    av = _num(nac["totales"], "actasContabilizadas", "avance")
    cont = _num(nac["totales"], "contabilizadas", "cont")
    total = _num(nac["totales"], "totalActas", "total")
    ncand = len(nac["candidatos"])
    print("  -> avance %s%%  (%d/%d actas)  - %d participantes en lista"
          % (av, int(cont or 0), int(total or 0), ncand))

    if solo_probe:
        print("\n[OK] Probe OK. Host, backend e idEleccion responden con datos.")
        print("  Quita --probe para correr la captura completa.")
        return

    snap_dir = os.path.join(out_dir, "snapshot_" + ts.strftime("%Y%m%dT%H%M%SZ"))
    os.makedirs(snap_dir, exist_ok=True)
    print("  Carpeta: %s" % snap_dir)

    capturas = {}
    for amb in ambitos_run:
        capturas[amb] = capturar_ambito(amb, incluir_distritos)

    totales_rows = [nac["totales"]]
    cand_rows = list(nac["candidatos"])
    for amb, cap in capturas.items():
        totales_rows.append(cap["ambito_summary"]["totales"])
        cand_rows.extend(cap["ambito_summary"]["candidatos"])
        for lvl in (cap["deps"], cap["provs"], cap["dists"]):
            for r in lvl:
                totales_rows.append(r["totales"])
                cand_rows.extend(r["candidatos"])

    iso = ts.isoformat()
    for r in totales_rows:
        r["snapshot_utc"] = iso
    for r in cand_rows:
        r["snapshot_utc"] = iso

    write_csv(os.path.join(snap_dir, "totales.csv"), totales_rows)
    write_csv(os.path.join(snap_dir, "candidatos.csv"), cand_rows)

    with open(os.path.join(snap_dir, "raw_nacional.json"), "w", encoding="utf-8") as f:
        json.dump(nac, f, ensure_ascii=False, indent=2, default=str)

    summary = {
        "snapshot_utc": iso, "host": HOST, "idEleccion": ID_ELECCION,
        "rows_totales": len(totales_rows), "rows_candidatos": len(cand_rows),
        "ambitos": [AMBITOS[a] for a in ambitos_run],
        "n_distritos_peru": capturas.get(1, {}).get("n_dists", 0),
        "n_ciudades_extranjero": capturas.get(2, {}).get("n_dists", 0),
        "avance_nacional_pct": av, "actas_contabilizadas": cont, "total_actas": total,
        "out_dir": snap_dir,
    }
    with open(os.path.join(snap_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)

    print("\n[OK] Snapshot completo")
    print("  Totales:    %d filas" % len(totales_rows))
    print("  Candidatos: %d filas" % len(cand_rows))
    print("  Avance nacional: %s%% (%d/%d actas)" % (av, int(cont or 0), int(total or 0)))
    resumen_pendientes(capturas)


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

def main():
    global HOST, BACKEND_PATH, BASE, HOME_URL, ROOT_URL, ORIGIN
    global ID_ELECCION, WORKERS, VERIFY_SSL, RATE_DELAY

    ap = argparse.ArgumentParser(
        description="ONPE 2026 segunda vuelta - scraper portable (solo stdlib)")
    ap.add_argument("--host", default=HOST)
    ap.add_argument("--backend", default=BACKEND_PATH)
    ap.add_argument("--id", type=int, default=None,
                    help="idEleccion (sacalo de DevTools o usa --probe-id)")
    ap.add_argument("--out", default="onpe_2v_out")
    ap.add_argument("--workers", type=int, default=WORKERS,
                    help="hilos concurrentes (default %d; baja a 2 si el WAF estrangula)" % WORKERS)
    ap.add_argument("--delay", type=float, default=RATE_DELAY,
                    help="gap base entre requests en segundos (default %.2f)" % RATE_DELAY)
    ap.add_argument("--no-distrito", action="store_true")
    ap.add_argument("--no-verify", action="store_true",
                    help="no verificar SSL (solo si la maquina tiene certs rotos)")
    ap.add_argument("--probe", action="store_true")
    ap.add_argument("--probe-id", action="store_true")
    ap.add_argument("--id-range", default="1,40")
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--solo-peru", action="store_true")
    grp.add_argument("--solo-extranjero", action="store_true")
    args = ap.parse_args()

    HOST = args.host
    BACKEND_PATH = args.backend
    BASE = "https://" + HOST + BACKEND_PATH
    HOME_URL = "https://" + HOST + "/main/presidenciales"
    ROOT_URL = "https://" + HOST + "/"
    ORIGIN = "https://" + HOST
    HEADERS["Referer"] = HOME_URL
    HEADERS["Origin"] = ORIGIN
    ID_ELECCION = args.id
    WORKERS = max(1, args.workers)
    RATE_DELAY = max(0.0, args.delay)
    VERIFY_SSL = not args.no_verify

    build_opener_global()

    if not args.probe_id and ID_ELECCION is None:
        ap.error("falta --id (idEleccion). Sacalo de DevTools o corre --probe-id primero.")

    start = time.time()
    if args.probe_id:
        lo, hi = (int(x) for x in args.id_range.split(","))
        probe_id(lo, hi)
    else:
        if args.solo_peru:
            ambitos = (1,)
        elif args.solo_extranjero:
            ambitos = (2,)
        else:
            ambitos = (1, 2)
        os.makedirs(args.out, exist_ok=True)
        run_snapshot(args.out, incluir_distritos=not args.no_distrito,
                     ambitos_run=ambitos, solo_probe=args.probe)
    print("\n[t] %.1fs total" % (time.time() - start))


if __name__ == "__main__":
    main()
