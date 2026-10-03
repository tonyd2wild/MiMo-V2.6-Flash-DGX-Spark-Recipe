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
//           Route option imageCap: false skips image trimming for that route and
//           streams the request body to the upstream instead of buffering it.
// Health:   GET /health
// Log:      proxy.log beside this file (trimmed requests, dropped upstreams, errors).

'use strict'

const http = require('http')
const https = require('https')
const fs = require('fs')
const path = require('path')
const { Transform } = require('stream')

const HOST = '127.0.0.1' // loopback only; the harness is the only client
const PORT = Number(process.env.IMAGE_CAP_PORT || 8905)
const MAX_BODY = 256 * 1024 * 1024 // images arrive base64-inlined
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

// `body` is a Buffer, or a readable stream on imageCap: false routes.
function forward(req, res, target, body, route = {}) {
  const streamingBody = typeof body.pipe === 'function'
  const headers = { ...req.headers }
  for (const h of ['host', 'connection', 'keep-alive', 'transfer-encoding', 'content-length']) delete headers[h]
  if (streamingBody) {
    // Unknown length (chunked upload): Node sends the upstream request chunked too.
    if (req.headers['content-length'] !== undefined) headers['content-length'] = req.headers['content-length']
  } else if (body.length || req.method === 'POST') headers['content-length'] = String(body.length)

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
    // The upstream answered before the upload finished (a 4xx, say): close the
    // client connection afterwards rather than leave the rest of the body unread.
    if (streamingBody && !req.complete) out.connection = 'close'
    res.writeHead(ur.statusCode || 502, out)
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
    if (!sse) { ur.pipe(res); return }
    const seen = new Set(); let buf = ''; let tripped = false; let ended = false; let lastId = null
    ur.on('data', (chunk) => {
      if (tripped) return
      buf += chunk.toString('utf8')
      let nl
      while ((nl = buf.indexOf('\n\n')) >= 0) {
        const evt = buf.slice(0, nl + 2); buf = buf.slice(nl + 2)
        const line = evt.trim()
        if (line.startsWith('data:') && !line.endsWith('[DONE]')) {
          try {
            const d = JSON.parse(line.slice(5))
            lastId = d.id || lastId
            for (const c of d.choices || []) for (const tc of (c.delta && c.delta.tool_calls) || []) seen.add(tc.index)
          } catch (e) {}
        }
        if (seen.size > toolCap) {
          tripped = true
          log(`${route.upstream}: tool-call storm guard tripped at ${seen.size} calls; cutting the response`)
          const fin = { id: lastId || 'guard', object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model: 'mimo-v2.6-flash', choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }] }
          res.write('data: ' + JSON.stringify(fin) + '\n\ndata: [DONE]\n\n'); res.end(); ended = true
          up.destroy()
          return
        }
        res.write(evt)
      }
    })
    ur.on('end', () => { if (!ended) { if (buf) res.write(buf); res.end() } })
  })

  up.on('error', (e) => {
    if (res.writableEnded) return // already answered, e.g. the 413 for an oversize streamed body
    log(`upstream ${target.host} unreachable: ${e.message}`)
    if (!res.headersSent) sendJson(res, 502, { error: { message: `upstream ${target.host} unreachable: ${e.message}` } })
    else res.destroy()
  })
  // The harness hung up (turn cancelled): stop the generation upstream too.
  res.on('close', () => { if (!res.writableFinished) up.destroy() })
  res.on('error', () => up.destroy())

  if (streamingBody) body.pipe(up)
  else up.end(body)
  return up
}

function rejectBody(req, res) {
  if (res.headersSent) { req.destroy(); res.destroy(); return }
  res.setHeader('connection', 'close')
  res.once('finish', () => req.destroy())
  sendJson(res, 413, { error: { message: 'request body too large for the image-cap proxy' } })
}

/**
 * imageCap: false routes. The body goes to the upstream as it arrives, through a
 * byte counter that enforces MAX_BODY, instead of being collected, parsed and
 * re-serialized: memory per request stays at a few chunks however large the
 * request (base64 images), and pipe() backpressure holds the client when the
 * upstream reads slowly. The response side (tool-call cap) is unchanged.
 */
function forwardUnmodified(req, res, target, route) {
  if (Number(req.headers['content-length']) > MAX_BODY) { rejectBody(req, res); return }
  let size = 0
  const limited = new Transform({
    transform(chunk, encoding, done) {
      size += chunk.length
      if (size > MAX_BODY) done(new Error('request body limit exceeded'))
      else done(null, chunk)
    },
  })
  const up = forward(req, res, target, limited, route)
  limited.on('error', () => {
    req.unpipe(limited); limited.unpipe(up)
    rejectBody(req, res)
    up.destroy()
  })
  req.on('error', (e) => { log(`client dropped mid-request: ${e.message}`); limited.destroy(); up.destroy() })
  // The harness hung up mid-upload: abort the upstream request too.
  res.on('close', () => {
    if (!req.complete) { req.unpipe(limited); limited.destroy(); up.destroy(); req.destroy() }
  })
  req.pipe(limited)
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
  if (route.imageCap === false) { forwardUnmodified(req, res, target, route); return }

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
  const summary = Object.entries(ROUTES).map(([k, r]) => `${k}->${r.upstream} (${r.imageCap === false ? 'streamed, no image cap' : `max ${r.maxImages}`})`).join(', ')
  log(`listening on http://${HOST}:${PORT} (pid ${process.pid})  ${summary}`)
  console.log(`image-cap proxy on http://${HOST}:${PORT}\n  ${summary}`)
})
