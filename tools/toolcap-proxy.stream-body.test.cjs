// Route option imageCap: false streams request bodies instead of buffering them.
// Run: node tools/toolcap-proxy.stream-body.test.cjs   (Node 18+, no dependencies)
//
// Runs a copy of the proxy in a temp directory (its own routes.json and proxy.log,
// nothing in tools/ is touched) in front of a mock upstream that hashes what it receives.

'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const http = require('node:http')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const crypto = require('node:crypto')
const { EventEmitter } = require('node:events')
const { spawn } = require('node:child_process')

const MIB = 1024 * 1024
const MAX_BODY = 256 * MIB // the proxy's limit

function freePort() {
  return new Promise((resolve) => {
    const s = http.createServer().listen(0, '127.0.0.1', () => { const { port } = s.address(); s.close(() => resolve(port)) })
  })
}

function within(promise, ms, what) {
  let timer
  return Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`timed out: ${what}`)), ms) })])
    .finally(() => clearTimeout(timer))
}

// Mock upstream: hashes each body as it streams in; /sse answers with a ten-call tool storm.
const upstreamEvents = new EventEmitter()
let upstreamRequests = 0
const upstream = http.createServer((req, res) => {
  upstreamRequests++
  const hash = crypto.createHash('sha256'); const parts = []; let bytes = 0
  req.on('data', (c) => {
    if (bytes === 0) upstreamEvents.emit('first')
    bytes += c.length; hash.update(c)
    if (bytes <= 4 * MIB) parts.push(c)
  })
  req.on('close', () => { if (!req.complete) upstreamEvents.emit('aborted') })
  req.on('end', () => {
    if (req.url.endsWith('/sse')) {
      res.writeHead(200, { 'content-type': 'text/event-stream' })
      for (let i = 0; i < 10; i++) {
        const tc = { index: i, id: `call_${i}`, type: 'function', function: { name: 'read', arguments: JSON.stringify({ n: i }) } }
        res.write('data: ' + JSON.stringify({ id: 'c1', object: 'chat.completion.chunk', choices: [{ index: 0, delta: { tool_calls: [tc] } }] }) + '\n\n')
      }
      return res.end('data: [DONE]\n\n')
    }
    let images = null
    try {
      const content = JSON.parse(Buffer.concat(parts)).messages[0].content
      images = { image: content.filter((p) => p.type === 'image_url').length, note: content.filter((p) => p.type === 'text').length }
    } catch {}
    res.writeHead(200, { 'content-type': 'application/json' })
    res.end(JSON.stringify({ bytes, sha256: hash.digest('hex'), images }))
  })
})

let proxyPort; let stopProxy
test.before(async () => {
  await new Promise((r) => upstream.listen(0, '127.0.0.1', r))
  const base = `http://127.0.0.1:${upstream.address().port}/v1`
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'toolcap-test-'))
  fs.copyFileSync(path.join(__dirname, 'toolcap-proxy.cjs'), path.join(dir, 'toolcap-proxy.cjs'))
  fs.writeFileSync(path.join(dir, 'routes.json'), JSON.stringify({ routes: {
    s: { upstream: base, maxImages: 2, imageCap: false },
    t: { upstream: base, maxImages: 2 },
  } }))
  proxyPort = await freePort()
  const child = spawn(process.execPath, [path.join(dir, 'toolcap-proxy.cjs')], { env: { ...process.env, IMAGE_CAP_PORT: String(proxyPort) } })
  await new Promise((resolve, reject) => {
    child.stdout.on('data', (d) => { if (String(d).includes('image-cap proxy on')) resolve() })
    child.on('exit', (code) => reject(new Error(`proxy exited ${code}`)))
  })
  stopProxy = () => { child.kill(); fs.rmSync(dir, { recursive: true, force: true }) }
})
test.after(() => { if (stopProxy) stopProxy(); upstream.closeAllConnections(); upstream.close() })

function request(route, urlPath, headers) {
  const req = http.request({ host: '127.0.0.1', port: proxyPort, path: `/${route}/v1${urlPath}`, method: 'POST', headers })
  const response = new Promise((resolve, reject) => {
    req.on('response', (res) => {
      const chunks = []
      res.on('data', (c) => chunks.push(c))
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString('utf8') }))
    })
    req.on('error', reject)
  })
  return { req, response }
}

const sha = (buf) => crypto.createHash('sha256').update(buf).digest('hex')

test('the upstream receives the body while the client is still sending it', async () => {
  const head = Buffer.alloc(1 * MIB, 'a'); const tail = Buffer.alloc(7 * MIB, 'b')
  const { req, response } = request('s', '/chat/completions', { 'content-type': 'application/json', 'content-length': String(head.length + tail.length) })
  const first = new Promise((r) => upstreamEvents.once('first', r))
  req.write(head)
  await within(first, 3000, 'upstream saw no bytes before the upload finished (body was buffered)')
  req.end(tail)
  const { status, body } = await within(response, 10000, 'response')
  assert.equal(status, 200)
  assert.deepEqual(JSON.parse(body), { bytes: 8 * MIB, sha256: sha(Buffer.concat([head, tail])), images: null })
})

test('chunked upload of unknown length arrives byte for byte', async () => {
  const payload = Buffer.from(JSON.stringify({ model: 'm', messages: [{ role: 'user', content: [{ type: 'text', text: 'café / 雪 / 😀 '.repeat(50000) }] }] }))
  const { req, response } = request('s', '/chat/completions', { 'content-type': 'application/json' })
  for (let i = 0; i < payload.length; i += 65537) req.write(payload.subarray(i, i + 65537)) // odd size: splits characters
  req.end()
  const { status, body } = await within(response, 10000, 'response')
  assert.equal(status, 200)
  const got = JSON.parse(body)
  assert.equal(got.bytes, payload.length)
  assert.equal(got.sha256, sha(payload))
})

test('a declared Content-Length over the limit gets 413 without contacting the upstream', async () => {
  const before = upstreamRequests
  const { req, response } = request('s', '/chat/completions', { 'content-type': 'application/json', 'content-length': String(MAX_BODY + 1) })
  req.flushHeaders()
  const { status } = await within(response, 3000, 'no early 413 for a declared oversize body')
  req.destroy()
  assert.equal(status, 413)
  assert.equal(upstreamRequests, before)
})

test('a client that hangs up mid-upload aborts the upstream request', async () => {
  const { req, response } = request('s', '/chat/completions', { 'content-type': 'application/json', 'content-length': String(8 * MIB) })
  response.catch(() => {})
  const first = new Promise((r) => upstreamEvents.once('first', r))
  const aborted = new Promise((r) => upstreamEvents.once('aborted', r))
  req.write(Buffer.alloc(1 * MIB, 'a'))
  await within(first, 3000, 'upstream saw no bytes')
  req.destroy()
  await within(aborted, 3000, 'upstream request was not aborted')
})

test('the tool-call cap still applies to responses on a streamed route', async () => {
  const { req, response } = request('s', '/sse', { 'content-type': 'application/json' })
  req.end('{}')
  const { body } = await within(response, 10000, 'response')
  const indices = new Set()
  for (const evt of body.split('\n\n')) {
    const line = evt.trim()
    if (!line.startsWith('data:') || line.endsWith('[DONE]')) continue
    for (const c of JSON.parse(line.slice(5)).choices) for (const tc of (c.delta.tool_calls || [])) indices.add(tc.index)
  }
  assert.equal(indices.size, 6)
  assert.ok(body.endsWith('data: [DONE]\n\n'))
})

test('control: routes without the option still trim images', async () => {
  const image = { type: 'image_url', image_url: { url: 'data:image/png;base64,AAAA' } }
  const { req, response } = request('t', '/chat/completions', { 'content-type': 'application/json' })
  req.end(JSON.stringify({ model: 'm', messages: [{ role: 'user', content: [image, image, image] }] }))
  const { body } = await within(response, 10000, 'response')
  assert.deepEqual(JSON.parse(body).images, { image: 2, note: 1 })
})
