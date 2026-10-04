// UTF-8 characters split across upstream network reads must reach the client intact.
// Run: node tools/toolcap-proxy.utf8.test.cjs   (Node 18+, no dependencies)
//
// Runs a copy of the proxy in a temp directory (its own routes.json and proxy.log,
// nothing in tools/ is touched) in front of a mock upstream that writes one SSE
// stream in two pieces, cut inside a multi-byte character.

'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const http = require('node:http')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawn } = require('node:child_process')

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

function freePort() {
  return new Promise((resolve) => {
    const s = http.createServer().listen(0, '127.0.0.1', () => { const { port } = s.address(); s.close(() => resolve(port)) })
  })
}

async function startProxy(upstreamPort) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'toolcap-test-'))
  fs.copyFileSync(path.join(__dirname, 'toolcap-proxy.cjs'), path.join(dir, 'toolcap-proxy.cjs'))
  fs.writeFileSync(path.join(dir, 'routes.json'), JSON.stringify({ routes: { t: { upstream: `http://127.0.0.1:${upstreamPort}/v1`, maxImages: 4 } } }))
  const port = await freePort()
  const child = spawn(process.execPath, [path.join(dir, 'toolcap-proxy.cjs')], { env: { ...process.env, IMAGE_CAP_PORT: String(port) } })
  await new Promise((resolve, reject) => {
    child.stdout.on('data', (d) => { if (String(d).includes('image-cap proxy on')) resolve() })
    child.on('exit', (code) => reject(new Error(`proxy exited ${code}`)))
  })
  return { port, stop: () => { child.kill(); fs.rmSync(dir, { recursive: true, force: true }) } }
}

// The upstream writes `pieces` as separate writes with a pause between, so they
// reach the proxy as separate reads.
function startUpstream(pieces) {
  return new Promise((resolve) => {
    const server = http.createServer(async (req, res) => {
      req.resume()
      await new Promise((r) => req.on('end', r))
      res.writeHead(200, { 'content-type': 'text/event-stream' })
      for (const p of pieces) { res.write(p); await sleep(30) }
      res.end()
    }).listen(0, '127.0.0.1', () => resolve(server))
  })
}

function post(port) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, path: '/t/v1/chat/completions', method: 'POST', headers: { 'content-type': 'application/json' } }, (res) => {
      const chunks = []
      res.on('data', (c) => chunks.push(c))
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
    })
    req.on('error', reject)
    req.end(JSON.stringify({ model: 'm', stream: true, messages: [{ role: 'user', content: 'hi' }] }))
  })
}

// Reassemble what an OpenAI-style client would see.
function assemble(sse) {
  let content = ''; const calls = {}
  for (const evt of sse.split('\n\n')) {
    const line = evt.trim()
    if (!line.startsWith('data:') || line.endsWith('[DONE]')) continue
    const d = JSON.parse(line.slice(5))
    for (const c of d.choices || []) {
      if (c.delta && c.delta.content) content += c.delta.content
      for (const tc of (c.delta && c.delta.tool_calls) || []) {
        calls[tc.index] = calls[tc.index] || { name: '', args: '' }
        if (tc.function && tc.function.name) calls[tc.index].name += tc.function.name
        if (tc.function && tc.function.arguments) calls[tc.index].args += tc.function.arguments
      }
    }
  }
  return { content, calls }
}

const event = (delta) => 'data: ' + JSON.stringify({ id: 'c1', object: 'chat.completion.chunk', choices: [{ index: 0, delta }] }) + '\n\n'

// Split a byte buffer inside the first occurrence of `char` (after its first byte).
function splitInside(buf, char) {
  const at = buf.indexOf(Buffer.from(char)) + 1
  assert.ok(at > 0)
  return [buf.subarray(0, at), buf.subarray(at)]
}

async function run(pieces) {
  const upstream = await startUpstream(pieces)
  try {
    const proxy = await startProxy(upstream.address().port)
    try { return assemble(await post(proxy.port)) } finally { proxy.stop() }
  } finally { upstream.close() }
}

const TEXT = 'café / 雪 / 😀'

test('content split inside a 2-, 3- and 4-byte character', async () => {
  const body = Buffer.from(event({ role: 'assistant', content: TEXT }) + 'data: [DONE]\n\n')
  for (const ch of ['é', '雪', '😀']) {
    assert.equal((await run(splitInside(body, ch))).content, TEXT, `split inside ${ch}`)
  }
})

test('tool-call arguments split inside a character', async () => {
  const args = JSON.stringify({ path: 'résumé.txt', text: TEXT })
  const body = Buffer.from(
    event({ tool_calls: [{ index: 0, id: 'call_0', type: 'function', function: { name: 'write', arguments: '' } }] }) +
    event({ tool_calls: [{ index: 0, function: { arguments: args } }] }) + 'data: [DONE]\n\n')
  const { calls } = await run(splitInside(body, 'é'))
  assert.deepEqual(JSON.parse(calls[0].args), { path: 'résumé.txt', text: TEXT })
})

test('control: whole events, nothing split', async () => {
  const body = Buffer.from(event({ role: 'assistant', content: TEXT }) + 'data: [DONE]\n\n')
  assert.equal((await run([body])).content, TEXT)
})
