// Tool-call cap with several tool-call deltas in one SSE event.
// Run: node tools/toolcap-proxy.batched.test.cjs   (Node 18+, no dependencies)
//
// Runs a copy of the proxy in a temp directory (its own routes.json and proxy.log,
// nothing in tools/ is touched) in front of a mock upstream that replays a fixed stream.

'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const http = require('node:http')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawn } = require('node:child_process')

function freePort() {
  return new Promise((resolve) => {
    const s = http.createServer().listen(0, '127.0.0.1', () => { const { port } = s.address(); s.close(() => resolve(port)) })
  })
}

async function startProxy(upstreamPort) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'toolcap-test-'))
  fs.copyFileSync(path.join(__dirname, 'toolcap-proxy.cjs'), path.join(dir, 'toolcap-proxy.cjs'))
  fs.writeFileSync(path.join(dir, 'routes.json'), JSON.stringify({ routes: { t: { upstream: `http://127.0.0.1:${upstreamPort}/v1`, maxImages: 4, toolCap: 6 } } }))
  const port = await freePort()
  const child = spawn(process.execPath, [path.join(dir, 'toolcap-proxy.cjs')], { env: { ...process.env, IMAGE_CAP_PORT: String(port) } })
  await new Promise((resolve, reject) => {
    child.stdout.on('data', (d) => { if (String(d).includes('image-cap proxy on')) resolve() })
    child.on('exit', (code) => reject(new Error(`proxy exited ${code}`)))
  })
  return { port, stop: () => { child.kill(); fs.rmSync(dir, { recursive: true, force: true }) } }
}

function startUpstream(body) {
  return new Promise((resolve) => {
    const server = http.createServer((req, res) => {
      req.resume()
      req.on('end', () => { res.writeHead(200, { 'content-type': 'text/event-stream' }); res.end(body) })
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

async function run(body) {
  const upstream = await startUpstream(body)
  try {
    const proxy = await startProxy(upstream.address().port)
    try { return await post(proxy.port) } finally { proxy.stop() }
  } finally { upstream.close() }
}

// What an OpenAI-style client assembles from the stream.
function assemble(sse) {
  const calls = {}; let finish = null; let done = false
  for (const evt of sse.split('\n\n')) {
    const line = evt.trim()
    if (line === 'data: [DONE]') { done = true; continue }
    if (!line.startsWith('data:')) continue
    for (const c of JSON.parse(line.slice(5)).choices || []) {
      if (c.finish_reason) finish = c.finish_reason
      for (const tc of (c.delta && c.delta.tool_calls) || []) {
        calls[tc.index] = calls[tc.index] || { name: '', args: '' }
        if (tc.function && tc.function.name) calls[tc.index].name += tc.function.name
        if (tc.function && tc.function.arguments) calls[tc.index].args += tc.function.arguments
      }
    }
  }
  return { calls, finish, done }
}

const event = (toolCalls, extra = {}) =>
  'data: ' + JSON.stringify({ id: 'c1', object: 'chat.completion.chunk', choices: [{ index: 0, delta: { tool_calls: toolCalls }, ...extra }] }) + '\n\n'
const start = (i) => ({ index: i, id: `call_${i}`, type: 'function', function: { name: 'read', arguments: '' } })
const args = (i, text) => ({ index: i, function: { arguments: text } })

function expectCalls(sse, n) {
  const { calls, finish, done } = assemble(sse)
  assert.deepEqual(Object.keys(calls).map(Number), [...Array(n).keys()], 'forwarded call indices')
  for (let i = 0; i < n; i++) {
    let parsed
    try { parsed = JSON.parse(calls[i].args) } catch { assert.fail(`call ${i} arguments arrived incomplete: ${calls[i].args}`) }
    assert.deepEqual(parsed, { n: i })
  }
  assert.equal(finish, 'tool_calls')
  assert.ok(done, 'stream ends with [DONE]')
}

test('eight calls in one event, cap 6: the first six are forwarded', async () => {
  const all = [...Array(8).keys()].map((i) => ({ ...start(i), function: { name: 'read', arguments: JSON.stringify({ n: i }) } }))
  expectCalls(await run(event(all) + 'data: [DONE]\n\n'), 6)
})

test('the event that starts call 7 also finishes call 6: call 6 arrives complete', async () => {
  let body = ''
  for (let i = 0; i < 6; i++) body += event([start(i)]) + event([args(i, '{"n":')]) + (i < 5 ? event([args(i, `${i}}`)]) : '')
  body += event([args(5, '5}'), start(6)]) + event([args(6, '{"n":6}')]) + 'data: [DONE]\n\n'
  expectCalls(await run(body), 6)
})

test('control: one call per event, ten calls, cap 6', async () => {
  let body = ''
  for (let i = 0; i < 10; i++) body += event([start(i)]) + event([args(i, JSON.stringify({ n: i }))])
  expectCalls(await run(body + 'data: [DONE]\n\n'), 6)
})

test('control: under the cap the stream is forwarded byte for byte', async () => {
  const body = event([start(0), start(1)]) + event([args(0, '{"n":0}'), args(1, '{"n":1}')]) +
    event([], { finish_reason: 'tool_calls' }) + 'data: [DONE]\n\n'
  assert.equal(await run(body), body)
})
