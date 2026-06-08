/*
 * conteoONPE - crawler de consola - SEGUNDA VUELTA 2026
 * =====================================================
 * Corre dentro del navegador (DevTools > Console) estando en:
 *   https://resultadosegundavuelta.onpe.gob.pe/main/presidenciales
 *
 * Por que en el navegador: usa tu sesion real, asi esquiva el WAF, y no
 * necesita Python ni instalar nada. Al terminar descarga un snapshot
 * onpe_data_<fecha>_<avance>pct.json listo para la carpeta snapshots/ del
 * dashboard (mismo formato; sin pasar por CSV ni build_data).
 *
 * Captura hasta nivel distrito (Peru) y ciudad (extranjero). idEleccion = 10.
 * Incluye, por nodo: avance, participacion, actas contabilizadas/total,
 * votos por candidato, emitidos/validos, y el desglose JEE
 * (enviadasJee / pendientesJee = actas observadas en el jurado).
 *
 * Uso: pegar todo y Enter. No tocar la pestania mientras corre (~minutos).
 * El resultado queda tambien en window.__SNAP por si queres inspeccionarlo.
 */
(async () => {
  const ID = 10, BASE = "/presentacion-backend", POOL = 8;
  const LR = "TODOS,PERÚ,EXTRANJERO";
  const sleep = ms => new Promise(r => setTimeout(r, ms));

  async function jget(path, tries = 4) {
    let last;
    for (let t = 0; t < tries; t++) {
      try {
        const r = await fetch(BASE + path, { headers: { Accept: "application/json" }, credentials: "include" });
        if (!r.ok) throw new Error("HTTP " + r.status);
        const ct = r.headers.get("content-type") || "";
        if (!ct.toLowerCase().includes("json")) throw new Error("no-JSON " + ct);
        return await r.json();
      } catch (e) { last = e; await sleep(300 * (t + 1) + Math.random() * 400); }
    }
    throw last;
  }
  const qs = o => "?" + new URLSearchParams(o).toString();

  async function pool(items, size, worker) {
    let i = 0, done = 0; const out = new Array(items.length), total = items.length;
    async function run() {
      while (i < items.length) {
        const idx = i++;
        try { out[idx] = await worker(items[idx], idx); } catch (e) { out[idx] = null; }
        done++; if (done % 100 === 0 || done === total) console.log("  ... " + done + "/" + total);
      }
    }
    await Promise.all(Array.from({ length: size }, run));
    return out;
  }

  function totalesPath(n) {
    if (n.level === 0) return "/resumen-general/totales" + qs({ idEleccion: ID, tipoFiltro: "eleccion" });
    const o = { idAmbitoGeografico: n.ambito, idEleccion: ID, tipoFiltro: "ubigeo_nivel_0" + n.level, idUbigeoDepartamento: n.dep };
    if (n.level >= 2) o.idUbigeoProvincia = n.prov;
    if (n.level >= 3) o.idUbigeoDistrito = n.dist;
    return "/resumen-general/totales" + qs(o);
  }
  function candsPath(n) {
    if (n.level === 0) return "/eleccion-presidencial/participantes-ubicacion-geografica-nombre" + qs({ idEleccion: ID, tipoFiltro: "eleccion" });
    const o = { tipoFiltro: "ubigeo_nivel_0" + n.level, idAmbitoGeografico: n.ambito, ubigeoNivel1: n.dep, listRegiones: LR, idEleccion: ID };
    if (n.level >= 2) o.ubigeoNivel2 = n.prov;
    if (n.level >= 3) o.ubigeoNivel3 = n.dist;
    return "/eleccion-presidencial/participantes-ubicacion-geografica-nombre" + qs(o);
  }

  console.log("ONPE 2V crawler - idEleccion " + ID);
  console.log("1) descubriendo arbol territorial...");
  const nodes = { __root__: { nombre: "PERÚ", nivel: "nacional", parent: null, ambito: 0, children: [] } };
  const toFetch = [{ key: "__root__", level: 0, ambito: 0 }];

  for (const ambito of [1, 2]) {
    const deps = (await jget("/ubigeos/departamentos" + qs({ idEleccion: ID, idAmbitoGeografico: ambito }))).data || [];
    for (const d of deps) {
      nodes[d.ubigeo] = { nombre: d.nombre, nivel: "departamento", parent: "__root__", ubigeo: d.ubigeo, ambito, children: [] };
      nodes.__root__.children.push(d.ubigeo);
      toFetch.push({ key: d.ubigeo, level: 1, ambito, dep: d.ubigeo });
    }
    const provLists = await pool(deps, POOL, async d =>
      ({ d, ps: (await jget("/ubigeos/provincias" + qs({ idEleccion: ID, idAmbitoGeografico: ambito, idUbigeoDepartamento: d.ubigeo }))).data || [] }));
    const allProvs = [];
    for (const pl of provLists) { if (!pl) continue; for (const p of pl.ps) {
      nodes[p.ubigeo] = { nombre: p.nombre, nivel: "provincia", parent: pl.d.ubigeo, ubigeo: p.ubigeo, ambito, children: [] };
      nodes[pl.d.ubigeo].children.push(p.ubigeo);
      toFetch.push({ key: p.ubigeo, level: 2, ambito, dep: pl.d.ubigeo, prov: p.ubigeo });
      allProvs.push({ dep: pl.d.ubigeo, p });
    } }
    const distLists = await pool(allProvs, POOL, async x =>
      ({ x, ds: (await jget("/ubigeos/distritos" + qs({ idEleccion: ID, idAmbitoGeografico: ambito, idUbigeoProvincia: x.p.ubigeo }))).data || [] }));
    for (const dl of distLists) { if (!dl) continue; for (const dd of dl.ds) {
      nodes[dd.ubigeo] = { nombre: dd.nombre, nivel: "distrito", parent: dl.x.p.ubigeo, ubigeo: dd.ubigeo, ambito, children: [] };
      nodes[dl.x.p.ubigeo].children.push(dd.ubigeo);
      toFetch.push({ key: dd.ubigeo, level: 3, ambito, dep: dl.x.dep, prov: dl.x.p.ubigeo, dist: dd.ubigeo });
    } }
  }

  console.log("   nodos a capturar: " + toFetch.length);
  console.log("2) capturando totales + candidatos (pool " + POOL + ")...");
  const data = {}; let master = null;
  await pool(toFetch, POOL, async n => {
    const [tot, cand] = await Promise.all([jget(totalesPath(n)), jget(candsPath(n))]);
    const t = tot.data || {}, list = cand.data || [], votes = {};
    for (const c of list) votes[String(c.codigoAgrupacionPolitica)] = c.totalVotosValidos || 0;
    data[n.key] = {
      av: t.actasContabilizadas, part: t.participacionCiudadana,
      cont: t.contabilizadas, total: t.totalActas, votes,
      emit: t.totalVotosEmitidos, valid: t.totalVotosValidos,
      enviadasJee: t.enviadasJee, pendientesJee: t.pendientesJee
    };
    if (n.key === "__root__") master = list.map(c => ({ code: String(c.codigoAgrupacionPolitica), agr: c.nombreAgrupacionPolitica, cand: c.nombreCandidato }));
  });

  const root = data.__root__ || {}, ts = new Date().toISOString();
  const avance = root.av != null ? root.av : null;
  const snap = { snapshot: { ts, label: new Date().toLocaleString("es-PE"), avance }, candidatos_master: master || [], nodes, data };
  console.log("3) master:", master);
  console.log("   nodos con data: " + Object.keys(data).length + " / " + toFetch.length + " | avance " + avance + "%");
  if (root.enviadasJee != null) console.log("   pendientes en jee: enviadas " + root.enviadasJee + " / pendientes " + root.pendientesJee);
  const pct = avance != null ? avance.toFixed(2) : "NA";
  const stamp = ts.replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");
  const fname = "onpe_data_" + stamp + "_" + pct + "pct.json";
  const blob = new Blob([JSON.stringify(snap)], { type: "application/json" });
  const a = document.createElement("a"); a.href = URL.createObjectURL(blob); a.download = fname;
  document.body.appendChild(a); a.click(); a.remove();
  window.__SNAP = snap;
  console.log("LISTO: descargado " + fname);
})();
