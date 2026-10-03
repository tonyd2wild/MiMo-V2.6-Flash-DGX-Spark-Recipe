// Image-cap proxy: keeps every model request inside the model server's
// per-request image limit, so a session can never be killed by one image too many.
//
// Why this exists
// ---------------
// vLLM refuses a request carrying more images than its --limit-mm-per-prompt:
//
//   400 At most 4 image(s) may be provided in one prompt. (parameter=image)
//
// The limit is per REQUEST, and every turn resends the whole history. So once a
// session holds one image too many, every later turn fails -- compaction too,
// since it resends the history as well -- and the session is dead. AGENTS.md
// told agents this and they did it anyway (2026-09-10: one session called
// read_image six times against a 4-image model). This makes it impossible
// instead of forbidden: the NEWEST images are kept up to the limit, older ones
// are replaced by a short text note saying so, and the request goes through.
// A session that is already wedged starts working again on its next turn.
//
// Everything else passes through untouched, streaming included.
//
// EVERY model route goes through this process, so it must never die from one
// bad request: every stream has an error handler, and anything that still
// escapes is logged rather than allowed to exit (see the bottom of the file).
// start-hidden.vbs also restarts it if it does exit.
//
// Routes:   http://127.0.0.1:8905/<route>/v1/...  ->  <upstream>/...
// Limits:   routes.json beside this file. Measured, not guessed: each server was
//           sent 9 images and named its own limit in the refusal.
// Health:   GET /health
// Log:      proxy.log beside this file (trimmed requests, dropped upstreams, errors).

'use strict'

const http = require('http')
const https = require('https')
const fs = require('fs')
const path = require('path')
const { StringDecoder } = require('string_decoder')

const HOST = '127.0.0.1' // loopback only; the harness is the only client
const PORT = Number(process.env.IMAGE_CAP_PORT || 8905)
const MAX_BODY = 256 * 1024 * 1024 // images arrive base64-inlined
const MAX_SSE_EVENT_CHARS = 8 * 1024 * 1024 // bound an unfinished event, not the response length
const LOG_FILE = path.join(__dirname, 'proxy.log')
const ROUTES = JSON.parse(fs.readFileSync(path.join(__dirname, 'routes.json'), 'utf8')).routes

// Exit code meaning "another copy already serves this port". start-hidden.vbs
// stops its restart loop on it instead of retrying forever.
const EXIT_PORT_TAKEN = 3

// OpenAI chat uses image_url; the other two spellings cover clients that send
// the Responses-style or Anthropic-style part through a compatible endpoint.
const IMAGE_PART_TYPES = new Set(['image_url', 'image', 'input_image'])

const agents = {
  'http:': new http.Agent({ keepAlive: true }),
  'https:': new https.Agent({ keepAlive: true }),
}

function log(line) {
  try { fs.appendFileSync(LOG_FILE, `${new Date().toISOString()} ${line}\n`) } catch { /* logging must never break a request */ }
}

function sendJson(res, status, obj) {
  if (res.headersSent || res.destroyed) return
  const body = Buffer.from(JSON.stringify(obj))
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': body.length })
  res.end(body)
}

/**
 * Keep the newest `max` image parts across all messages; replace the older
 * ones, in place, with a text note. Newest-first because the latest image is
 * almost always the one the current turn is about.
 */
function capImages(messages, max) {
  const refs = []
  messages.forEach((msg, mi) => {
    if (msg && Array.isArray(msg.content)) {
      msg.content.forEach((part, pi) => {
        if (part && IMAGE_PART_TYPES.has(part.type)) refs.push([mi, pi])
      })
    }
  })
  const excess = refs.length - max
  if (excess <= 0) return { total: refs.length, removed: 0 }

  const note =
    `[An earlier image was removed from this request: this model accepts at most ${max} ` +
    `image(s) per request and the conversation holds ${refs.length}. The newest ${max} are ` +
    'still attached. If you need an older one again, use analyze_image on its file, which ' +
    'returns text and costs no image slot.]'
  for (const [mi, pi] of refs.slice(0, excess)) messages[mi].content[pi] = { type: 'text', text: note }
  return { total: refs.length, removed: excess }
}

// SSE line rules: https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation
// Normalize line endings, preserving every field. Only data fields form the JSON payload.
async function* sseEvents(stream) {
  const decoder = new StringDecoder('utf8')
  let first = true; let skipLF = false; let line = ''; let lines = []; let data = []; let size = 0
  for await (const chunk of stream) {
    let text = typeof chunk === 'string' ? chunk : decoder.write(chunk)
    if (!text) continue
    if (first) { first = false; text = text.replace(/^\uFEFF/, '') }
    if (skipLF) text = text.replace(/^\n/, '')
    skipLF = text.endsWith('\r')
    const parts = text.replace(/\r\n|\r/g, '\n').split('\n')
    for (let i = 0; i < parts.length; i++) {
      const ended = i < parts.length - 1
      size += parts[i].length + (ended ? 1 : 0)
      if (size > MAX_SSE_EVENT_CHARS) throw new Error('upstream SSE event exceeds the size limit')
      line += parts[i]
      if (!ended) continue
      lines.push(line + '\n')
      if (line === '') {
        yield { raw: lines.join(''), data: data.join('\n') }
        lines = []; data = []; size = 0
      } else if (line === 'data') data.push('')
      else if (line.startsWith('data:')) data.push(line.slice(5).replace(/^ /, ''))
      line = ''
    }
  }
  // Never turn a truncated data event into a successful completion. Trailing
  // comments/metadata without a data field are discarded, as SSE specifies.
  if (decoder.end() || data.length || line === 'data' || line.startsWith('data:')) {
    throw new Error('upstream ended with an incomplete SSE event')
  }
}

async function writeSse(res, text) {
  if (res.destroyed) return false
  if (!res.write(text)) {
    await new Promise((resolve) => {
      const ready = () => {
        res.off('drain', ready); res.off('close', ready); res.off('error', ready)
        resolve()
      }
      res.once('drain', ready); res.once('close', ready); res.once('error', ready)
      if (res.destroyed) ready()
    })
  }
  return !res.destroyed
}

function forward(req, res, target, body, route = {}) {
  const headers = { ...req.headers }
  for (const h of ['host', 'connection', 'keep-alive', 'transfer-encoding', 'content-length']) delete headers[h]
  if (body.length || req.method === 'POST') headers['content-length'] = String(body.length)
  // The cap must inspect the decoded SSE. Do not advertise encodings we cannot inspect.
  if (route.toolCap === undefined || route.toolCap > 0) headers['accept-encoding'] = 'identity'

  const lib = target.protocol === 'https:' ? https : http
  const up = lib.request({
    protocol: target.protocol,
    hostname: target.hostname,
    port: target.port || (target.protocol === 'https:' ? 443 : 80),
    path: target.pathname + target.search,
    method: req.method,
    headers,
    agent: agents[target.protocol],
  }, (ur) => {
    const out = { ...ur.headers }
    for (const h of ['connection', 'keep-alive', 'transfer-encoding']) delete out[h]
    // An upstream that drops MID-RESPONSE (the tailnet going away does exactly
    // this) makes `ur` emit 'error'. pipe() does not forward it, and an
    // unhandled stream error kills the whole process -- every route with it.
    // The first version had no handler here; it was found dead on 2026-09-13
    // with this PC's tailnet logged out (cause not proven -- it had also been
    // started from a Claude chat, whose processes can die with the chat).
    // End just this response.
    ur.on('error', (e) => { log(`upstream ${target.host} dropped mid-response: ${e.message}`); res.destroy() })
    // Tool-call storm guard (2026-09-22): MiMo-V2.6-Flash sometimes emits hundreds of
    // tool calls in one response (it plans an imagined trajectory). Cap the number of
    // distinct tool calls per streamed response; past the cap, stop the generation
    // upstream and close the SSE cleanly so the harness executes what it has and
    // asks again with real results. Route option toolCap (default 6, 0 = off).
    const toolCap = route.toolCap === undefined ? 6 : route.toolCap
    const sse = toolCap > 0 && String(ur.headers['content-type'] || '').startsWith('text/event-stream')
    if (!sse) { res.writeHead(ur.statusCode || 502, out); ur.pipe(res); return }
    const encoding = String(ur.headers['content-encoding'] || 'identity').trim().toLowerCase()
    if (encoding !== 'identity') {
      sendJson(res, 502, { error: { message: 'guarded SSE requires an identity-encoded upstream response' } })
      ur.destroy(); up.destroy()
      return
    }
    // Framing normalization and a cap cut can both change the response length.
    delete out['content-length']
    const seen = new Set(); let lastId = null
    const startResponse = () => { if (!res.headersSent) res.writeHead(ur.statusCode || 502, out) }
    const relay = async () => {
      try {
        for await (const frame of sseEvents(ur)) {
          if (res.destroyed) return
          if (frame.data && frame.data !== '[DONE]') {
            const d = JSON.parse(frame.data)
            lastId = d.id || lastId
            for (const c of d.choices || []) for (const tc of (c.delta && c.delta.tool_calls) || []) seen.add(tc.index)
          }
          startResponse()
          if (seen.size > toolCap) {
            log(`${route.upstream}: tool-call storm guard tripped at ${seen.size} calls; cutting the response`)
            const fin = { id: lastId || 'guard', object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model: 'mimo-v2.6-flash', choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }] }
            await writeSse(res, 'data: ' + JSON.stringify(fin) + '\n\ndata: [DONE]\n\n')
            res.end(); up.destroy()
            return
          }
          if (!await writeSse(res, frame.raw)) return
        }
        startResponse(); res.end()
      } catch (error) {
        log(`upstream SSE rejected: ${error.message}`)
        if (!res.headersSent) sendJson(res, 502, { error: { message: 'invalid or incomplete upstream SSE response' } })
        else res.destroy()
        ur.destroy(); up.destroy()
      }
    }
    relay()
  })

  up.on('error', (e) => {
    log(`upstream ${target.host} unreachable: ${e.message}`)
    if (!res.headersSent) sendJson(res, 502, { error: { message: `upstream ${target.host} unreachable: ${e.message}` } })
    else res.destroy()
  })
  // The harness hung up (turn cancelled): stop the generation upstream too.
  res.on('close', () => { if (!res.writableFinished) up.destroy() })
  res.on('error', () => up.destroy())

  up.end(body)
}

const server = http.createServer((req, res) => {
  if (req.method === 'GET' && (req.url === '/health' || req.url === '/')) {
    return sendJson(res, 200, { status: 'ok', port: PORT, pid: process.pid, routes: ROUTES })
  }

  const m = req.url.match(/^\/([^/?]+)\/v1(\/.*|\?.*)?$/)
  const route = m && ROUTES[m[1]]
  if (!route) {
    return sendJson(res, 404, { error: { message: `unknown route ${req.url}; expected /<route>/v1/... with route one of: ${Object.keys(ROUTES).join(', ')}` } })
  }
  const target = new URL(route.upstream.replace(/\/+$/, '') + (m[2] || ''))

  const chunks = []
  let size = 0
  let aborted = false
  // The harness can drop the connection while still sending the body.
  req.on('error', (e) => { aborted = true; log(`client dropped mid-request: ${e.message}`) })
  req.on('data', (c) => {
    if (aborted) return
    size += c.length
    if (size > MAX_BODY) {
      aborted = true
      sendJson(res, 413, { error: { message: 'request body too large for the image-cap proxy' } })
      req.destroy()
    } else chunks.push(c)
  })
  req.on('end', () => {
    if (aborted) return
    let body = Buffer.concat(chunks)
    if (req.method === 'POST' && body.length) {
      try {
        const payload = JSON.parse(body.toString('utf8'))
        if (payload && Array.isArray(payload.messages)) {
          const r = capImages(payload.messages, route.maxImages)
          if (r.removed) {
            body = Buffer.from(JSON.stringify(payload))
            log(`${m[1]}: request carried ${r.total} images; kept newest ${route.maxImages}, replaced ${r.removed} with a note`)
          }
        }
      } catch { /* not JSON (or compressed): pass it through untouched */ }
    }
    forward(req, res, target, body, route)
  })
})

// Generations can run for minutes; never cut a request off on a timer here.
server.requestTimeout = 0
server.headersTimeout = 60000
server.on('clientError', (e, socket) => { try { socket.destroy() } catch {} })

server.on('error', (e) => {
  log(`server error: ${e.message}`)
  console.error(e.message)
  process.exit(e.code === 'EADDRINUSE' ? EXIT_PORT_TAKEN : 1)
})

// Last line of defence. This is a stateless relay: one request going wrong in
// a way nothing above anticipated must cost that request, not every model route.
process.on('uncaughtException', (e) => log(`uncaught (kept running): ${(e && e.stack) || e}`))
process.on('unhandledRejection', (e) => log(`unhandled rejection (kept running): ${(e && e.stack) || e}`))

server.listen(PORT, HOST, () => {
  const summary = Object.entries(ROUTES).map(([k, r]) => `${k}->${r.upstream} (max ${r.maxImages})`).join(', ')
  log(`listening on http://${HOST}:${PORT} (pid ${process.pid})  ${summary}`)
  console.log(`image-cap proxy on http://${HOST}:${PORT}\n  ${summary}`)
})
