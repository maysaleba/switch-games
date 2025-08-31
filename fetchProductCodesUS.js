#!/usr/bin/env node
/**
 * US enrichment (MASTER + SNAPSHOT, no platform logic)
 *
 * - MASTER (union): keeps everything ever seen; never drops rows when base changes.
 * - CURRENT snapshot: only items present in today's base (active_in_base=true).
 * - Non-destructive merge: base blanks can't wipe enriched values.
 * - PRESERVE SENTINEL: productCode_us === "" stays "", never reprocessed.
 *
 * Reads:
 *   data/us_games.json                  <-- from fetch_us.js (Algolia)
 *   data/us_games_enriched.json         <-- MASTER (if exists)
 * Writes:
 *   data/us_games_enriched.json         <-- MASTER (union, never drops)
 *   data/us_games_enriched_current.json <-- SNAPSHOT (active only)
 */

const fs = require("fs");
const path = require("path");
const https = require("https");

const IN_PATH     = "data/us_games.json";
const OUT_MASTER  = "data/us_games_enriched.json";
const OUT_CURRENT = "data/us_games_enriched_current.json";

const CONFIG = { concurrency: 4, baseDelayMs: 900, retries: 2, rps: 0.8 };

const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36";
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: CONFIG.concurrency * 2 });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = (ms, spread=0.7) => ms + Math.floor(Math.random()*Math.max(1, ms*spread));
const ensureDir = (p) => fs.mkdirSync(path.dirname(p), { recursive: true });

// --- helpers ---
function loadJsonSafe(p, def) { try { return JSON.parse(fs.readFileSync(p,"utf8")); } catch { return def; } }
function saveJsonPretty(p, obj) { ensureDir(p); fs.writeFileSync(p, JSON.stringify(obj,null,2)); }
const nowIso = () => new Date().toISOString();

function pick(a,b) { const sa=(a??"").toString().trim(); const sb=(b??"").toString().trim(); return sa?a:(sb?b:a); }
function pickCode(baseVal, priorVal) {
  if (typeof baseVal==="string" && baseVal.trim()!=="") return baseVal;
  if (priorVal==="") return "";
  if (priorVal!==undefined) return priorVal;
  return baseVal;
}

// --- simple fetcher ---
async function fetchText(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { "User-Agent": UA }, agent: httpsAgent }, (res) => {
      let html=""; res.setEncoding("utf8");
      res.on("data",(c)=>html+=c);
      res.on("end",()=>resolve({html,status:res.statusCode||0}));
    }).on("error",reject);
  });
}

function extractHACAnywhere(text) {
  if (!text) return null;
  const m = String(text).match(/\bHAC[0-9A-Z\-]{4,}\b/i);
  return m?m[0].toUpperCase():null;
}
function extractProductCodeFromHtml(html) {
  const m1 = html.match(/<meta[^>]+itemprop=["']sku["'][^>]+content=["']([^"']+)["']/i);
  if (m1&&m1[1]) return m1[1].trim();
  const m2 = html.match(/itemprop=["']sku["'][^>]*>\s*([^<>\s][^<]*)\s*</i);
  if (m2&&m2[1]) return m2[1].trim();
  const m3 = html.match(/>\s*(SKU|Model)\s*<\/[^>]+>[\s\S]{0,200}?([A-Z0-9\-_]{4,})/i);
  if (m3&&m3[2]) return m3[2].trim();
  return extractHACAnywhere(html);
}

// --- fetch per game ---
async function getProductCodeForUrl(url) {
  let html=null, err=null;
  try { const res=await fetchText(url); html=res.html; }
  catch(e){ err=e; }
  if (!html) return { url, productCode:null, result:"ERROR", error:String(err) };
  const code=extractProductCodeFromHtml(html);
  if (code) return { url, productCode:code, result:"FOUND" };
  return { url, productCode:"", result:"NO_CODE" };
}

function resolveUSUrl(raw) {
  if (!raw) return null;
  if (/^https?:\/\//i.test(raw)) return raw;
  return `https://www.nintendo.com${raw.startsWith("/")?"":"/"}${raw}`;
}

// --- concurrency runner ---
async function mapWithConcurrency(items, limit, fn, baseDelayMs=0) {
  const results=new Array(items.length); let idx=0,inflight=0;
  return new Promise((resolve)=>{
    const kick=()=>{ if(idx>=items.length && inflight===0) return resolve(results);
      while(inflight<limit && idx<items.length){
        const i=idx++; inflight++;
        (async()=>{ try{ if(baseDelayMs) await sleep(jitter(baseDelayMs,0.8)); results[i]=await fn(items[i],i);}
          catch(e){results[i]={error:String(e)}} finally{inflight--;kick();}})();
      }};
    kick();
  });
}

// --- MAIN ---
(async()=>{
  ensureDir(OUT_MASTER);

  const baseGames = loadJsonSafe(IN_PATH, []);
  const inBase = new Map();
  for(const g of Array.isArray(baseGames)?baseGames:[]){
    const id=String(g.nsuid_us||g.nsuid||g.objectID||"");
    if(!id) continue;
    inBase.set(id,g);
  }

  const existingMaster = loadJsonSafe(OUT_MASTER, []);
  const byNsuid=new Map();
  for(const g of Array.isArray(existingMaster)?existingMaster:[]){
    const id=String((g&& (g.nsuid_us||g.nsuid||g.objectID))||"");
    if(id) byNsuid.set(id,g);
  }

  const unionIds=new Set([...inBase.keys(),...byNsuid.keys()]);
  const working=[]; const now=nowIso();

  for(const id of unionIds){
    const base=inBase.get(id)||{}; const prior=byNsuid.get(id)||{};
    let merged={...prior};

    if(base.title)    merged.title=base.title;
    if(base.url)      merged.url=base.url;
    if(base.urlKey)   merged.urlKey=base.urlKey;
    if(base.platform) merged.platform=base.platform;

    merged.nsuid_us     = pick(base.nsuid_us, merged.nsuid_us);
    merged.genres       = (Array.isArray(base.genres)&&base.genres.length)?base.genres:(Array.isArray(merged.genres)?merged.genres:[]);
    merged.releaseDate  = pick(base.releaseDate, merged.releaseDate);
    merged.imageSquare  = pick(base.imageSquare, merged.imageSquare);
    merged.imageKey     = pick(base.imageKey, merged.imageKey);
    merged.publisher    = pick(base.publisher, merged.publisher);
    merged.dlcType      = pick(base.dlcType, merged.dlcType);
    merged.playerCount  = pick(base.playerCount, merged.playerCount);

    merged.active_in_base = inBase.has(id);
    merged.first_seen_at  = merged.first_seen_at || prior.first_seen_at || now;
    if(merged.active_in_base) merged.last_seen_at=now;
    else merged.last_seen_at=merged.last_seen_at||now;

    merged.productCode_us = pickCode(base.productCode_us, merged.productCode_us);

    working.push(merged);
  }

  const toProcessCode=working.filter(g=>{
    if(!g.active_in_base) return false;
    const code=g.productCode_us;
    if(code==="") return false;
    if(typeof code==="string" && code.trim()) return false;
    const url=resolveUSUrl(g.url); return !!url;
  });

  console.log(`[store.us MASTER] union=${working.length} active=${inBase.size} codeFetch=${toProcessCode.length}`);

  const idxByNsuid=new Map();
  for(let i=0;i<working.length;i++){
    const id=String(working[i].nsuid_us||working[i].nsuid||working[i].objectID||"");
    if(id) idxByNsuid.set(id,i);
  }

  let processed=0, found=0, markedEmpty=0, markedNull=0;

  await mapWithConcurrency(
    toProcessCode, Math.max(1,CONFIG.concurrency),
    async(game)=>{
      const url=resolveUSUrl(game.url);
      const res=await getProductCodeForUrl(url);
      const i=idxByNsuid.get(String(game.nsuid_us||game.nsuid||game.objectID));
      if(i!=null){
        working[i].productCode_us=res.productCode;
        working[i].last_checked_at=nowIso();
      }
      if(res.result==="FOUND"){found++;}
      else if(res.result==="NO_CODE"||res.result==="NOT_FOUND"){markedEmpty++;}
      else {markedNull++;}
      processed++;
      if(processed%10===0||processed===toProcessCode.length) saveJsonPretty(OUT_MASTER,working);
    },
    CONFIG.baseDelayMs
  );

  saveJsonPretty(OUT_MASTER,working);
  const current=working.filter(g=>g.active_in_base);
  saveJsonPretty(OUT_CURRENT,current);

  console.log(`Done. Processed=${processed}, FOUND=${found}, EMPTY="${markedEmpty}", NULL=${markedNull}. Master: ${OUT_MASTER} | Current: ${OUT_CURRENT}`);
})();
