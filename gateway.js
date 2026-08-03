/*
 * AVA EARS GATEWAY — Stage 1 (passthrough) with Stage-2 shadow hooks
 * ==================================================================
 * A Vapi custom-transcriber server: Vapi streams call audio here, we stream
 * it to Deepgram Nova-3 (with the production keyterms) and return transcripts
 * in Vapi's custom-transcriber format. Behaviorally identical to Vapi's
 * built-in Deepgram ears — this stage exists to prove the protocol and
 * measure added latency on the ZZ TEST rig before any ensemble logic.
 *
 * Stage 2 (optional, via env): if SPEECHMATICS_API_KEY is set, the caller
 * audio is ALSO streamed to Speechmatics real-time with a large custom
 * dictionary (the name corpus — far beyond Nova-3's keyterm budget), in
 * SHADOW MODE: its transcripts are only logged, with a DISAGREEMENT line
 * whenever the two engines differ on a final. Shadow mode never affects
 * what Vapi receives — it exists to measure, on real calls, how often the
 * deep ears would have saved a garble, before any fusion is switched on.
 *
 * ENV:
 *   DEEPGRAM_API_KEY   required
 *   PORT               default 8080
 *   KEYTERMS           comma-separated; defaults to the production 25
 *   SWAP_CHANNELS      "1" if customer/assistant come out swapped
 *   SPEECHMATICS_API_KEY  optional — enables shadow mode
 *   CORPUS_FILE        optional path to ava_name_corpus.txt (one name/line)
 *                      used as the Speechmatics additional_vocab
 *
 * Vapi assistant config to point the TEST RIG here (never prod first):
 *   transcriber: { provider: "custom-transcriber",
 *                  server: { url: "wss://<your-deploy>/transcriber" } }
 *
 * Protocol notes:
 *   Vapi → us: one JSON start message {encoding,sampleRate,channels}, then
 *              binary PCM linear16, 16kHz, 2 channels interleaved
 *              (channel 0 = customer, channel 1 = assistant).
 *   us → Vapi: {"type":"transcriber-response","transcription":"...",
 *               "channel":"customer"|"assistant","transcriptType":"partial"|"final"}
 */
'use strict';
const http = require('http');
const fs = require('fs');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 8080);
const DG_KEY = process.env.DEEPGRAM_API_KEY || '';
const SM_KEY = process.env.SPEECHMATICS_API_KEY || '';
const SWAP = process.env.SWAP_CHANNELS === '1';
const KEYTERMS = (process.env.KEYTERMS || [
  // production 25
  'Apex Transportation','GroundWidgets','Groundops','Santacruz','sedan','SUV',
  'Sprinter van','mini bus','motor coach','Ava','Kruti','Chudgar','Apurva','Patel',
  'Saluja','Sarabjit','Amar','Pant','JFK','LaGuardia','LGA','Newark','EWR',
  'Teterboro','Westchester',
  // travel pack v1 (Aug 2026) — hotels, airlines, geo; keep total under
  // Deepgram's 500-token keyterm budget (~syllables)
  'Marriott','Marquis','Hilton','Hyatt','Westin','Sheraton','Ritz-Carlton',
  'Waldorf Astoria','InterContinental','DoubleTree','Embassy Suites',
  'Lufthansa','Air India','Emirates','Etihad','Qatar Airways','JetBlue',
  'Frankfurt','Paramus','Hoboken','Secaucus','Weehawken','Hackensack',
  'Times Square','Rockefeller','Javits','Hudson Yards','Kalisa',
  'Flensburger','Strasse','Gmail'
].join(',')).split(',').map(s => s.trim()).filter(Boolean);

if (!DG_KEY) { console.error('FATAL: DEEPGRAM_API_KEY not set'); process.exit(1); }

let CORPUS = [];
if (process.env.CORPUS_FILE) {
  try {
    // Line format: "Content" or "Content | sounds1; sounds2" (sounds_like hints)
    CORPUS = fs.readFileSync(process.env.CORPUS_FILE, 'utf8')
      .split(/\r?\n/).map(s => s.trim()).filter(s => s && !s.startsWith('#')).slice(0, 1000)
      .map(line => {
        const [content, sl] = line.split('|').map(x => x.trim());
        return sl
          ? { content, sounds_like: sl.split(';').map(x => x.trim()).filter(Boolean) }
          : { content };
      });
    console.log(`corpus loaded: ${CORPUS.length} entries for Speechmatics additional_vocab`);
  } catch (e) { console.error('corpus file unreadable:', e.message); }
}

const log = (...a) => console.log(new Date().toISOString(), ...a);

// v3 additions (all optional, degrade gracefully if env unset):
//   WEBAPP_URL + WEBAPP_TOKEN  -> per-call dynamic vocab (feature #1)
//   GEMINI_API_KEY             -> shadow LLM arbiter on disagreements (feature #3)
const WEBAPP_URL = process.env.WEBAPP_URL || '';
const WEBAPP_TOKEN = process.env.WEBAPP_TOKEN || '';
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || '';

// v4: promote the arbiter from shadow to ACTIVE. When ARBITER_ACTIVE (default on),
// a suspect entity-like customer FINAL from Deepgram is held for up to HOLD_MS so
// Speechmatics (deep dictionary) + the Gemini arbiter can correct an entity garble
// (e.g. "Amar Pant" heard as "I'm not a bank") BEFORE Vapi's LLM ever sees it.
// Conservative + fail-open: a correction is forwarded ONLY when the arbiter's answer
// contains a corpus entity SM heard that DG missed; otherwise the original DG final
// is forwarded unchanged. Rig-only (prod uses Vapi's built-in Deepgram, not this
// gateway). Set ARBITER_ACTIVE=0 to revert to pure shadow (log-only).
const ARBITER_ACTIVE = process.env.ARBITER_ACTIVE !== '0';
const HOLD_MS = Number(process.env.ARBITER_HOLD_MS || 1200);
const SM_MAX_DELAY = Number(process.env.SM_MAX_DELAY || 1);

// Cheap phonetic key (Soundex-ish, no deps) for garble-tolerant entity matching.
function phKey(w) {
  w = String(w || '').toLowerCase().replace(/[^a-z]/g, '');
  if (!w) return '';
  const map = { b:'1',f:'1',p:'1',v:'1', c:'2',g:'2',j:'2',k:'2',q:'2',s:'2',x:'2',z:'2',
                d:'3',t:'3', l:'4', m:'5',n:'5', r:'6' };
  let out = w[0], prev = map[w[0]] || '';
  for (let i = 1; i < w.length; i++) { const c = map[w[i]] || ''; if (c && c !== prev) out += c; prev = c; }
  return (out + '000').slice(0, 4);
}
// Per-call phonetic index: phKey -> canonical entity token, from keyterms + corpus.
function buildEntityIndex(terms, corpus) {
  const byKey = new Map();
  const add = (s) => String(s || '').split(/\s+/).forEach(tok => {
    const t = tok.replace(/[^A-Za-z]/g, '');
    if (t.length >= 3) { const k = phKey(t); if (k && !byKey.has(k)) byKey.set(k, t); }
  });
  terms.forEach(add);
  corpus.forEach(c => add(c && c.content ? c.content : c));
  return byKey;
}
// Common conversational words that never need entity arbitration — a final made up
// only of these is forwarded instantly (keeps "yes"/"no"/"correct" snappy).
const STOPWORDS = new Set(('a an and the to of for is it im i m yes yeah yep no nope not '
  + 'ok okay sure right correct thanks thank you hi hello hey please me my your that this '
  + 'was were are be do dont don t can could would will just at on in as so um uh').split(/\s+/));
// Entity-like = a short answer (name/email/number) — the turns where garbles bite and a
// brief hold is acceptable. Sentences (>5 words or ending in .!?) and pure conversational
// fillers pass through with zero added latency.
function isEntityLike(t) {
  let s = String(t || '').trim(); if (!s) return false;
  if (/@|\d/.test(s)) return true;                          // emails / numbers: always
  // Deepgram smart_format punctuates almost every final ("Amar Pant."), so a lone
  // trailing .!? must NOT be treated as a sentence — strip it before judging length.
  s = s.replace(/[.!?]+$/, '').trim(); if (!s) return false;
  const words = s.split(/\s+/);
  if (words.length > 5 || /[.!?]/.test(s)) return false;    // real sentences (internal stops / >5 words) pass fast
  const nonStop = words.filter(w => !STOPWORDS.has(w.toLowerCase().replace(/[^a-z]/g, '')));
  return nonStop.length > 0;                                // hold only if a real token remains
}

// Pull the caller's phone number out of Vapi's start message (shape varies;
// try the known paths, log the keys so the real shape is captured on call 1).
function extractCallerNumber(m) {
  const paths = [
    m && m.call && m.call.customer && m.call.customer.number,
    m && m.customer && m.customer.number,
    m && m.call && m.call.from,
    m && m.from,
    m && m.phoneNumber,
  ];
  for (const p of paths) if (p && String(p).replace(/\D/g, '').length >= 7) return String(p);
  return '';
}

// Ask the webapp for caller-specific keyterms + vocab (feature #1).
async function fetchCallerVocab(phone) {
  if (!WEBAPP_URL || !WEBAPP_TOKEN || !phone) return { keyterms: [], vocab: [] };
  try {
    const u = WEBAPP_URL + '?token=' + encodeURIComponent(WEBAPP_TOKEN) +
      '&action=vocab&from_phone=' + encodeURIComponent(phone);
    const r = await fetch(u, { redirect: 'follow' });
    const j = await r.json();
    return { keyterms: Array.isArray(j.keyterms) ? j.keyterms : [],
             vocab: Array.isArray(j.vocab) ? j.vocab : [] };
  } catch (e) { console.error('vocab fetch failed:', e.message); return { keyterms: [], vocab: [] }; }
}

// Shadow LLM arbiter (feature #3): given the two ears' finals on a disputed
// utterance, ask Gemini for the most likely intended text. LOG ONLY — the
// live transcript Vapi receives is untouched. Promote later by piping the
// verdict into the transcriber-response.
async function geminiArbiter(dgText, smText, contextTerms) {
  if (!GEMINI_API_KEY) return null;
  try {
    const prompt =
      'Two speech recognizers disagree on one phone-call utterance from a limo booking. ' +
      'Output ONLY the single most likely intended text (names/addresses/emails matter most). ' +
      'Prefer a known term if one is phonetically close.\n' +
      'Recognizer A: "' + dgText + '"\nRecognizer B: "' + smText + '"\n' +
      'Known terms: ' + (contextTerms.slice(0, 40).join(', ') || '(none)') + '\nAnswer:';
    const u = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key=' + GEMINI_API_KEY;
    const r = await fetch(u, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }],
        generationConfig: { maxOutputTokens: 40, temperature: 0 } }),
    });
    const j = await r.json();
    const out = j && j.candidates && j.candidates[0] && j.candidates[0].content &&
      j.candidates[0].content.parts && j.candidates[0].content.parts[0] &&
      j.candidates[0].content.parts[0].text;
    return out ? out.trim() : null;
  } catch (e) { console.error('gemini arbiter failed:', e.message); return null; }
}

/* ---------- health endpoint (deploy platforms ping this) ---------- */
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('ava-ears-gateway ok\n');
});
const wss = new WebSocketServer({ server, path: '/transcriber' });

wss.on('connection', (vapi) => {
  const id = Math.random().toString(36).slice(2, 8);
  log(`[${id}] Vapi connected`);
  let sampleRate = 16000, channels = 2, closed = false;
  let dg = null, sm = null;
  let dgQueue = [];           // audio buffered until Deepgram is open
  let lastFinal = { customer: '', assistant: '' };
  let callKeyterms = KEYTERMS.slice();   // per-call: base + injected caller terms
  let callCorpus = CORPUS.slice();       // per-call: base + injected caller vocab
  let entityByKey = null;                // lazily-built phonetic entity index for this call
  let pendingCust = null;                // { dgText, timer, done } — a held suspect customer final
  let lastSM = { text: '', at: 0 };      // most recent Speechmatics transcript

  function forwardToVapi(channel, text, type) {
    if (!closed && vapi.readyState === WebSocket.OPEN) {
      vapi.send(JSON.stringify({ type: 'transcriber-response', transcription: text, channel, transcriptType: type }));
    }
  }
  // Hold an entity-like customer FINAL briefly; fail-open forwards the DG original.
  function holdCustomerFinal(dgText) {
    if (pendingCust && !pendingCust.done) resolveHold(pendingCust.dgText); // flush prior, keep order
    const p = { dgText, done: false, timer: null };
    pendingCust = p;
    p.timer = setTimeout(() => { if (!p.done) resolveHold(dgText); }, HOLD_MS);
  }
  function resolveHold(fallbackText) {
    if (!pendingCust || pendingCust.done) return;
    pendingCust.done = true;
    if (pendingCust.timer) clearTimeout(pendingCust.timer);
    forwardToVapi('customer', fallbackText, 'final');
  }
  // On an SM transcript, correct the held DG final IFF SM carries a corpus entity DG
  // missed AND the Gemini arbiter's answer contains that entity. Else leave it to the
  // timer (DG original). Conservative: never substitutes on the arbiter's word alone.
  async function tryArbitrate(smText) {
    const p = pendingCust;
    if (!p || p.done) return;
    if (!entityByKey) entityByKey = buildEntityIndex(callKeyterms, callCorpus);
    const dgTokens = new Set(p.dgText.toLowerCase().replace(/[^a-z0-9 ]/g, '').split(/\s+/).filter(Boolean));
    let hit = null;
    for (const raw of String(smText).split(/\s+/)) {
      const w = raw.replace(/[^A-Za-z]/g, ''); if (w.length < 3) continue;
      if (dgTokens.has(w.toLowerCase())) continue;
      const canon = entityByKey.get(phKey(w));
      if (canon) { hit = canon; break; }
    }
    if (!hit) return; // nothing corpus-backed to justify a correction
    const verdict = await geminiArbiter(p.dgText, smText, callKeyterms);
    if (p.done) return; // timer already forwarded the original
    if (verdict && verdict.toLowerCase().includes(hit.toLowerCase())) {
      p.done = true; if (p.timer) clearTimeout(p.timer);
      log(`[${id}] *** ARBITER (ACTIVE) substituted: "${verdict}"  [DG:"${p.dgText}" SM:"${smText}" entity:${hit}]`);
      forwardToVapi('customer', verdict, 'final');
    }
    // else: no confident correction — the hold timer forwards the DG original
  }

  /* ---------- Deepgram Nova-3 leg (the production ears) ---------- */
  function openDeepgram() {
    const qs = new URLSearchParams({
      model: 'nova-3', encoding: 'linear16',
      sample_rate: String(sampleRate), channels: String(channels),
      multichannel: 'true', interim_results: 'true',
      smart_format: 'true', punctuate: 'true', endpointing: '300',
    });
    for (const k of callKeyterms) qs.append('keyterm', k);
    dg = new WebSocket('wss://api.deepgram.com/v1/listen?' + qs.toString(),
      { headers: { Authorization: 'Token ' + DG_KEY } });
    dg.on('open', () => {
      log(`[${id}] deepgram open, flushing ${dgQueue.length} buffered chunks`);
      for (const b of dgQueue) dg.send(b);
      dgQueue = [];
    });
    dg.on('message', (raw) => {
      let m; try { m = JSON.parse(raw.toString()); } catch { return; }
      if (m.type !== 'Results' || !m.channel) return;
      const alt = m.channel.alternatives && m.channel.alternatives[0];
      const text = alt && alt.transcript;
      if (!text) return;
      const chIdx = Array.isArray(m.channel_index) ? m.channel_index[0] : 0;
      let channel = chIdx === 0 ? 'customer' : 'assistant';
      if (SWAP) channel = channel === 'customer' ? 'assistant' : 'customer';
      const type = m.is_final ? 'final' : 'partial';
      if (type === 'final') {
        lastFinal[channel] = text;
        log(`[${id}] DG final (${channel}): ${text}`);
        if (ARBITER_ACTIVE && SM_KEY && channel === 'customer' && isEntityLike(text)) {
          holdCustomerFinal(text); // forwarded later — corrected or (fail-open) original
          return;
        }
      }
      forwardToVapi(channel, text, type);
    });
    dg.on('error', (e) => log(`[${id}] deepgram error:`, e.message));
    dg.on('close', (c) => { log(`[${id}] deepgram closed (${c})`); if (!closed) setTimeout(() => { if (!closed) openDeepgram(); }, 500); });
  }

  /* ---------- Speechmatics shadow leg (stage 2, log-only) ---------- */
  function openSpeechmatics() {
    if (!SM_KEY) return;
    // Speechmatics real-time v2: wss with jwt/apikey auth via header.
    sm = new WebSocket('wss://eu2.rt.speechmatics.com/v2', {
      headers: { Authorization: 'Bearer ' + SM_KEY },
    });
    sm.on('open', () => {
      sm.send(JSON.stringify({
        message: 'StartRecognition',
        audio_format: { type: 'raw', encoding: 'pcm_s16le', sample_rate: sampleRate },
        transcription_config: {
          language: 'en', enable_partials: false, max_delay: SM_MAX_DELAY,
          additional_vocab: callCorpus,
        },
      }));
      log(`[${id}] speechmatics shadow open (${callCorpus.length}-name dictionary)`);
    });
    sm.on('message', (raw) => {
      let m; try { m = JSON.parse(raw.toString()); } catch { return; }
      if (m.message !== 'AddTranscript') return;
      const text = (m.metadata && m.metadata.transcript || '').trim();
      if (!text) return;
      lastSM = { text, at: Date.now() };
      log(`[${id}] SM ${ARBITER_ACTIVE ? 'active' : 'shadow'} (customer): ${text}`);
      // ACTIVE: if a suspect customer final is being held, arbitrate it now.
      if (ARBITER_ACTIVE && pendingCust && !pendingCust.done) { tryArbitrate(text); return; }
      // SHADOW (log-only) disagreement probe when not actively holding.
      const norm = (s) => s.toLowerCase().replace(/[^a-z0-9 ]/g, '').split(/\s+/).filter(Boolean);
      const a = new Set(norm(lastFinal.customer)); const b = norm(text);
      const uniques = b.filter(w => w.length > 3 && !a.has(w));
      if (uniques.length) {
        log(`[${id}] *** DISAGREEMENT candidates: ${uniques.join(' ')} (SM heard these, DG final did not)`);
        geminiArbiter(lastFinal.customer, text, callKeyterms).then(v => {
          if (v) log(`[${id}] *** ARBITER (shadow): "${v}"  [A:"${lastFinal.customer}" B:"${text}"]`);
        });
      }
    });
    sm.on('error', (e) => log(`[${id}] speechmatics error:`, e.message));
    sm.on('close', (c) => log(`[${id}] speechmatics closed (${c})`));
  }

  /* Extract mono customer channel (ch 0) from interleaved stereo for SM. */
  function customerMono(buf) {
    const out = Buffer.alloc(buf.length / 2);
    for (let i = 0, o = 0; i + 3 < buf.length; i += 4, o += 2) {
      out[o] = buf[SWAP ? i + 2 : i]; out[o + 1] = buf[SWAP ? i + 3 : i + 1];
    }
    return out;
  }

  vapi.on('message', (data, isBinary) => {
    if (!isBinary) {
      let m; try { m = JSON.parse(data.toString()); } catch { return; }
      log(`[${id}] start message keys:`, Object.keys(m || {}).join(','));
      if (m.sampleRate) sampleRate = m.sampleRate;
      if (m.channels) channels = m.channels;
      // Feature #1: per-call dynamic vocab. Fetch caller-specific terms, merge,
      // THEN open the recognizer legs so the injected terms take effect.
      const phone = extractCallerNumber(m);
      if (phone) log(`[${id}] caller number detected for vocab injection`);
      fetchCallerVocab(phone).then(v => {
        if (v.keyterms.length) {
          for (const k of v.keyterms) if (!callKeyterms.includes(k)) callKeyterms.push(k);
          log(`[${id}] injected ${v.keyterms.length} per-call keyterms (total ${callKeyterms.length})`);
        }
        if (v.vocab.length) {
          const have = new Set(callCorpus.map(c => (c.content || '').toLowerCase()));
          for (const w of v.vocab) if (!have.has(String(w).toLowerCase())) callCorpus.push({ content: w });
          log(`[${id}] injected ${v.vocab.length} per-call vocab entries (total ${callCorpus.length})`);
        }
      }).catch(() => {}).finally(() => {
        openDeepgram();
        openSpeechmatics();
      });
      return;
    }
    // binary PCM
    if (dg && dg.readyState === WebSocket.OPEN) dg.send(data);
    else dgQueue.push(data);
    if (sm && sm.readyState === WebSocket.OPEN && channels === 2) {
      sm.send(customerMono(data));
    }
  });

  vapi.on('close', () => {
    closed = true;
    try { if (pendingCust && pendingCust.timer) clearTimeout(pendingCust.timer); } catch {}
    log(`[${id}] Vapi disconnected`);
    try { dg && dg.close(); } catch {}
    try { sm && sm.send(JSON.stringify({ message: 'EndOfStream', last_seq_no: 0 })); sm && sm.close(); } catch {}
  });
  vapi.on('error', (e) => log(`[${id}] vapi ws error:`, e.message));
});

server.listen(PORT, () => log(`ava-ears-gateway listening on :${PORT} (ws path /transcriber)`));
