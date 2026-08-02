# Ava Ears Gateway — Stage 1 + Shadow Stage 2

A Vapi **custom transcriber** server. Stage 1: passthrough to Deepgram Nova-3
with the production keyterms (behaviorally identical to today's ears — proves
the protocol, measures latency). Stage 2 (optional): Speechmatics runs in
parallel on the caller channel with the full name corpus as its dictionary,
in **shadow mode** — logged only, never affecting the call — so real calls
measure how often deep ears would have saved a garble before any fusion.

## Deploy (Railway — ~$5/mo hobby plan, always-on)

1. Install Node LTS (nodejs.org) if not present, then in a terminal:
   ```
   npm install -g @railway/cli
   railway login
   cd ava-ears-gateway
   railway init          # create a new project
   railway up            # deploys this folder
   ```
2. In the Railway dashboard → your service → **Variables**, add:
   - `DEEPGRAM_API_KEY` = your Deepgram key
   - (later, stage 2) `SPEECHMATICS_API_KEY` = Speechmatics key
   - (later, stage 2) `CORPUS_FILE` = `./ava_name_corpus.txt` — put the file
     in this folder before `railway up`
3. Railway → Settings → **Networking → Generate Domain**. Your endpoint is:
   `wss://<generated-domain>/transcriber`
4. Open `https://<generated-domain>/` in a browser — it should say
   `ava-ears-gateway ok`.

(Alternative hosts: Fly.io, Render paid tier. Avoid free tiers that sleep —
a sleeping gateway = dead air on a live call.)

## Point the TEST RIG at it (NEVER prod first)

PATCH the ZZ TEST rig (`43afdbc4-ff3c-47d9-8fb4-146474fad1e4`) from a
dashboard.vapi.ai tab:

```js
fetch('https://api.vapi.ai/assistant/43afdbc4-ff3c-47d9-8fb4-146474fad1e4', {
  method: 'PATCH',
  headers: { 'Authorization': 'Bearer <VAPI_KEY>', 'Content-Type': 'application/json' },
  body: JSON.stringify({
    transcriber: { provider: 'custom-transcriber',
                   server: { url: 'wss://<generated-domain>/transcriber' } }
  })
}).then(r => r.json()).then(j => console.log(j.transcriber))
```

## Rollback (one PATCH — memorize this)

```js
body: JSON.stringify({ transcriber: {
  provider: 'deepgram', model: 'nova-3', language: 'en',
  keyterm: [ /* the 25 production keyterms */ ] } })
```
(Or copy `transcriber` from the prod assistant, which stays untouched.)

## Test plan

1. Stage 1 A/B on the rig: same July-30 script through the gateway vs prod.
   Compare turn gaps (Vapi call ledger) and garble rate. Expect ~identical
   behavior with tens of ms added latency. Railway region: pick US-East
   (Vapi + Deepgram are US; a EU region adds ~100ms each way).
2. If clean → add `SPEECHMATICS_API_KEY` + corpus file, redeploy, run real
   test calls, then read the gateway logs (`railway logs`) for
   `*** DISAGREEMENT` lines — that's the measured value of the deep ears.
3. Only if disagreements show real saves → build fusion (stage 3): promote
   Speechmatics from shadow to arbiter for names/spellings/digits, and emit
   a disagreement flag the prompt's gates can consume.

## Notes

- Channels: Vapi sends 16kHz stereo PCM — channel 0 customer, channel 1
  assistant. If transcripts come out attributed to the wrong side, set env
  `SWAP_CHANNELS=1` instead of editing code.
- Keyterms are env-overridable (`KEYTERMS=a,b,c`); default = the production 25.
- The gateway auto-reconnects its Deepgram leg if it drops mid-call.
- Keep this folder private: env vars hold keys; never commit them to a repo.
