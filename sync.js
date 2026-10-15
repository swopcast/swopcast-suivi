'use strict';
/* Service de synchronisation des tâches (Vercel Function).
   Stocke l'état dans Upstash Redis (REST). Protégé par un code partagé (variable STUDIO_CODE).
   Fusion par tâche : la version la plus récente (updatedAt) l'emporte ; les suppressions sont gardées
   sous forme de « tombstones ». Le client renvoie son état complet à chaque synchro (fusion idempotente). */
const crypto = require('crypto');
const KEYNAME = 'swopcast:tasks:v1';
const STATUS = ['a-faire', 'en-cours', 'a-valider', 'bloque', 'termine'];
const CHANTIERS = ['P', 'A', 'B'];
const MAX_BYTES = 900 * 1024;

const ts = v => { const n = Date.parse(String(v || '')); return isNaN(n) ? 0 : n; };

function cfg() {
  return {
    url: process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL,
    token: process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN,
    code: process.env.STUDIO_CODE
  };
}
async function redis(c, cmd) {
  const r = await fetch(c.url, { method: 'POST', headers: { Authorization: 'Bearer ' + c.token, 'content-type': 'application/json' }, body: JSON.stringify(cmd) });
  if (!r.ok) throw new Error('redis ' + r.status);
  const j = await r.json();
  if (j.error) throw new Error(j.error);
  return j.result;
}
function sameCode(a, b) {
  const ha = crypto.createHash('sha256').update(String(a || '')).digest();
  const hb = crypto.createHash('sha256').update(String(b || '')).digest();
  return crypto.timingSafeEqual(ha, hb);
}
function cleanTask(t) {
  if (!t || typeof t !== 'object') return null;
  if (typeof t.id !== 'string' || !t.id || t.id.length > 40) return null;
  if (typeof t.title !== 'string' || !t.title.trim() || t.title.length > 300) return null;
  if (!STATUS.includes(t.status) || !CHANTIERS.includes(t.chantier)) return null;
  if (t.due && !/^\d{4}-\d{2}-\d{2}$/.test(t.due)) return null;
  if (!ts(t.updatedAt)) return null;
  return t;
}
function cleanDeleted(d) {
  const out = {};
  if (d && typeof d === 'object' && !Array.isArray(d)) {
    Object.entries(d).slice(0, 2000).forEach(([k, v]) => { if (k.length <= 40 && typeof v === 'string' && ts(v)) out[k] = v; });
  }
  return out;
}
function uniq(list) {
  const out = [];
  list.forEach(n => { n = String(n || '').trim().slice(0, 40); if (n && !out.some(x => x.toLowerCase() === n.toLowerCase())) out.push(n); });
  return out.slice(0, 30);
}
function mergeStates(L, R) {
  const del = Object.assign({}, L.deleted || {});
  Object.entries(R.deleted || {}).forEach(([id, t]) => { if (ts(t) > ts(del[id])) del[id] = t; });
  const map = new Map();
  (L.tasks || []).forEach(t => map.set(t.id, t));
  (R.tasks || []).forEach(t => { const c = map.get(t.id); if (!c || ts(t.updatedAt) > ts(c.updatedAt)) map.set(t.id, t); });
  const tasks = [];
  map.forEach((t, id) => { if (del[id] && ts(del[id]) >= ts(t.updatedAt)) return; tasks.push(t); });
  return { tasks, deleted: del, people: uniq((L.people || []).concat(R.people || [])) };
}

async function handler(req, res) {
  res.setHeader('Cache-Control', 'no-store');
  if (req.method !== 'POST') { res.status(405).json({ error: 'method' }); return; }
  const c = cfg();
  if (!c.url || !c.token || !c.code) { res.status(503).json({ error: 'not_configured' }); return; }
  if (!sameCode(req.headers['x-studio-code'], c.code)) {
    await new Promise(r => setTimeout(r, 500));
    res.status(401).json({ error: 'code' }); return;
  }
  let body = req.body;
  if (typeof body === 'string') { try { body = JSON.parse(body); } catch (e) { body = null; } }
  if (!body || !Array.isArray(body.tasks) || body.tasks.length > 500) { res.status(400).json({ error: 'payload' }); return; }
  const incoming = {
    tasks: body.tasks.map(cleanTask).filter(Boolean),
    deleted: cleanDeleted(body.deleted),
    people: Array.isArray(body.people) ? body.people.filter(x => typeof x === 'string') : []
  };
  try {
    const raw = await redis(c, ['GET', KEYNAME]);
    let stored = { tasks: [], deleted: {}, people: [] };
    if (raw) { try { stored = JSON.parse(raw); } catch (e) { /* valeur illisible : repart de l'état entrant */ } }
    const merged = mergeStates(
      { tasks: (stored.tasks || []).filter(cleanTask), deleted: cleanDeleted(stored.deleted), people: stored.people || [] },
      incoming);
    const next = JSON.stringify(merged);
    if (next.length > MAX_BYTES) { res.status(413).json({ error: 'too_large' }); return; }
    if (next !== raw) await redis(c, ['SET', KEYNAME, next]);
    res.status(200).json(merged);
  } catch (e) {
    res.status(502).json({ error: 'storage' });
  }
}
module.exports = handler;
module.exports.mergeStates = mergeStates;
