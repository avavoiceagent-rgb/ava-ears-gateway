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
  'Apex Transportation','GroundWidgets','Groundops','Santacruz','sedan','SUV',
  'Sprinter van','mini bus','motor coach','Ava','Kruti','Chudgar','Apurva','Patel',
  'Saluja','Sarabjit','Amar','Pant','JFK','LaGuardia','LGA','Newark','EWR',
  'Teterboro','Westchester'
].join(',')).split(',').map(s => s.trim()).filter(Boolean);

if (!DG_KEY) { console.error('FATAL: DEEPGRAM_API_KEY not set'); process.exit(1); }

let CORPUS = [];
if (process.env.CORPUS_FILE) {
  try {
    CORPUS = fs.readFileSync(process.env.CORPUS_FILE, 'utf8')
      .split(/\r?\n/).map(s => s.trim()).filter(Boolean).slice(0, 1000);
    console.log(`corpus loaded: ${CORPUS.length} names for Speechmatics additional_vocab`);
  } catch (e) { console.error('corpus file unreadable:', e.message); }
}

const log = (...a) => console.log(new Date().toISOString(), ...a);

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

  /* ---------- Deepgram Nova-3 leg (the production ears) ---------- */
  function openDeepgram() {
    const qs = new URLSearchParams({
      model: 'nova-3', encoding: 'linear16',
      sample_rate: String(sampleRate), channels: String(channels),
      multichannel: 'true', interim_results: 'true',
      smart_format: 'true', punctuate: 'true', endpointing: '300',
    });
    for (const k of KEYTERMS) qs.append('keyterm', k);
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
      }
      if (!closed && vapi.readyState === WebSocket.OPEN) {
        vapi.send(JSON.stringify({
          type: 'transcriber-response',
          transcription: text, channel, transcriptType: type,
        }));
      }
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
          language: 'en', enable_partials: false, max_delay: 2,
          additional_vocab: CORPUS.map(c => ({ content: c })),
        },
      }));
      log(`[${id}] speechmatics shadow open (${CORPUS.length}-name dictionary)`);
    });
    sm.on('message', (raw) => {
      let m; try { m = JSON.parse(raw.toString()); } catch { return; }
      if (m.message !== 'AddTranscript') return;
      const text = (m.metadata && m.metadata.transcript || '').trim();
      if (!text) return;
      log(`[${id}] SM shadow (customer): ${text}`);
      // Naive disagreement probe: token-level containment check vs last DG final.
      const norm = (s) => s.toLowerCase().replace(/[^a-z0-9 ]/g, '').split(/\s+/).filter(Boolean);
      const a = new Set(norm(lastFinal.customer)); const b = norm(text);
      const uniques = b.filter(w => w.length > 3 && !a.has(w));
      if (uniques.length) log(`[${id}] *** DISAGREEMENT candidates: ${uniques.join(' ')} (SM heard these, DG final did not)`);
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
      log(`[${id}] start message:`, JSON.stringify(m).slice(0, 200));
      if (m.sampleRate) sampleRate = m.sampleRate;
      if (m.channels) channels = m.channels;
      openDeepgram();
      openSpeechmatics();
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
    log(`[${id}] Vapi disconnected`);
    try { dg && dg.close(); } catch {}
    try { sm && sm.send(JSON.stringify({ message: 'EndOfStream', last_seq_no: 0 })); sm && sm.close(); } catch {}
  });
  vapi.on('error', (e) => log(`[${id}] vapi ws error:`, e.message));
});

server.listen(PORT, () => log(`ava-ears-gateway listening on :${PORT} (ws path /transcriber)`));
