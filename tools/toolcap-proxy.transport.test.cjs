'use strict'
// Run with Node 18.2+: node --test tools/toolcap-proxy.transport.test.cjs
// Isolated loopback servers, synthetic tool calls, no dependencies or model access.
const test = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const http = require('node:http')
const os = require('node:os')
const path = require('node:path')
const { spawn } = require('node:child_process')
const { gzipSync } = require('node:zlib')

const handlers = new Map()
const sockets = new Set()
const upstream = http.createServer((req, res) => {
  req.on('error', () => {})
  res.on('error', () => {})
  req.resume()
  req.once('end', () => handlers.get(req.url)?.(req, res))
})
upstream.on('connection', socket => {
  sockets.add(socket)
  socket.once('close', () => sockets.delete(socket))
})
let child; let dir; let proxyPort; let sequence = 0

function timeout(promise, ms = 3000, label = 'operation') {
  let timer
  return Promise.race([promise, new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out`)), ms)
  })]).finally(() => clearTimeout(timer))
}

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })
  return server.address().port
}

test.before(async () => {
  const port = await listen(upstream)
  const reservation = http.createServer()
  proxyPort = await listen(reservation)
  await new Promise(resolve => reservation.close(resolve))
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'toolcap-transport-'))
  fs.copyFileSync(path.join(__dirname, 'toolcap-proxy.cjs'), path.join(dir, 'toolcap-proxy.cjs'))
  fs.writeFileSync(path.join(dir, 'routes.json'), JSON.stringify({ routes: {
    cap: { upstream: `http://127.0.0.1:${port}/v1`, maxImages: 4, toolCap: 6 },
    off: { upstream: `http://127.0.0.1:${port}/v1`, maxImages: 4, toolCap: 0 },
  } }))
  child = spawn(process.execPath, [path.join(dir, 'toolcap-proxy.cjs')], {
    env: { ...process.env, IMAGE_CAP_PORT: String(proxyPort) }, stdio: ['ignore', 'pipe', 'pipe'],
  })
  let stdout = ''; let stderr = ''
  child.stderr.on('data', part => { stderr += part })
  await timeout(new Promise((resolve, reject) => {
    child.once('error', reject)
    child.once('exit', code => reject(new Error(`proxy exited ${code}: ${stderr}`)))
    child.stdout.on('data', part => {
      stdout += part
      if (stdout.includes('image-cap proxy on')) resolve()
    })
  }), 3000, 'proxy startup')
})

test.after(async () => {
  if (child && child.exitCode === null && child.signalCode === null) {
    const exited = new Promise(resolve => child.once('exit', resolve))
    child.kill()
    await timeout(exited, 1000, 'proxy shutdown').catch(() => child.kill('SIGKILL'))
  }
  for (const socket of sockets) socket.destroy()
  await new Promise(resolve => upstream.close(resolve))
  if (dir) fs.rmSync(dir, { recursive: true, force: true })
})

function request(handler, { route = 'cap', onData, paused = false } = {}) {
  const key = `/v1/test-${++sequence}`
  handlers.set(key, handler)
  let response
  const req = http.request({ host: '127.0.0.1', port: proxyPort,
    path: `/${route}${key}`, method: 'POST',
    headers: { 'content-type': 'application/json', 'accept-encoding': 'gzip, br' },
  })
  const complete = new Promise((resolve, reject) => {
    req.once('error', reject)
    req.once('response', res => {
      response = res
      const chunks = []
      res.on('error', reject)
      res.on('data', chunk => { chunks.push(chunk); onData?.(chunk, res) })
      res.once('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }))
      if (paused) res.pause()
    })
  })
  complete.catch(() => {})
  req.end(JSON.stringify({ model: 'fixture', stream: true, messages: [] }))
  const stop = () => { response?.destroy(); req.destroy(); handlers.delete(key) }
  return { complete, stop, req }
}

async function exchange(handler, options) {
  const client = request(handler, options)
  try { return await timeout(client.complete) } finally { client.stop() }
}

const chunk = (index, text = JSON.stringify({ item: index })) => JSON.stringify({
  id: 'synthetic', object: 'chat.completion.chunk', model: 'fixture',
  choices: [{ index: 0, delta: { tool_calls: [{ index, id: `call_${index}`, type: 'function',
    function: { name: 'inspect_item', arguments: text } }] }, finish_reason: null }],
})

function encodeFrame(data, style = 'lf') {
  const eol = style === 'crlf' ? '\r\n' : style === 'cr' ? '\r' : '\n'
  const prefix = style === 'comment' ? ': heartbeat\n' : style === 'event' ? 'event: message\n' : ''
  if (style === 'multiline' && data !== '[DONE]') {
    const comma = data.indexOf(',"choices"')
    return `data: ${data.slice(0, comma + 1)}\ndata: ${data.slice(comma + 1)}\n\n`
  }
  return `${prefix}data: ${data}${eol}${eol}`
}

function storm(style = 'lf') {
  return Array.from({ length: 8 }, (_, i) => encodeFrame(chunk(i), style)).join('') + encodeFrame('[DONE]', style)
}

function decode(body) {
  return body.toString().replace(/^\uFEFF/, '').replace(/\r\n|\r/g, '\n').split('\n\n')
    .map(frame => frame.split('\n').filter(line => line.startsWith('data:'))
      .map(line => line.slice(5).replace(/^ /, '')).join('\n'))
    .filter(Boolean).map(data => data === '[DONE]' ? data : JSON.parse(data))
}

function assertSix(response) {
  assert.equal(response.status, 200)
  const events = decode(response.body)
  const calls = events.filter(event => event !== '[DONE]').flatMap(event =>
    (event.choices || []).flatMap(choice => choice.delta?.tool_calls || []))
  assert.deepEqual(calls.map(call => call.index), [0, 1, 2, 3, 4, 5])
  for (const call of calls) assert.deepEqual(JSON.parse(call.function.arguments), { item: call.index })
  assert.equal(events.at(-1), '[DONE]')
  assert.equal(events.at(-2).choices[0].finish_reason, 'tool_calls')
}

for (const style of ['lf', 'crlf', 'cr', 'comment', 'event', 'multiline']) {
  test(`six complete calls with ${style} framing`, async () => {
    assertSix(await exchange((req, res) => {
      res.writeHead(200, { 'content-type': 'text/event-stream' })
      res.end(storm(style))
    }))
  })
}

test('ordinary LF frames below the cap remain byte-identical, including metadata', async () => {
  const body = ': comment\n\nevent: message\nid: sequence-1\nretry: 1000\n' +
    encodeFrame(chunk(0)) + 'data:\n\n' + encodeFrame('[DONE]')
  const result = await exchange((req, res) => {
    res.writeHead(200, { 'content-type': 'text/event-stream' }); res.end(body)
  })
  assert.equal(result.body.toString(), body)
})

test('split CRLF and UTF-8 characters arrive intact before upstream EOF', async () => {
  const args = JSON.stringify({ text: 'café / 雪 / 😀' })
  const wire = Buffer.from('\uFEFF' + encodeFrame(chunk(0, args), 'crlf'))
  const slices = [wire.subarray(0, 1), wire.subarray(1, 8), wire.subarray(8, wire.indexOf(Buffer.from('é')) + 1),
    wire.subarray(wire.indexOf(Buffer.from('é')) + 1, wire.length - 1), wire.subarray(wire.length - 1)]
  let finish; let release
  const received = new Promise(resolve => { release = resolve })
  const client = request((req, res) => {
    finish = () => res.end(encodeFrame('[DONE]', 'crlf'))
    res.writeHead(200, { 'content-type': 'text/event-stream' })
    const send = i => {
      if (i === slices.length || res.destroyed) return
      res.write(slices[i]); setTimeout(() => send(i + 1), 15)
    }
    send(0)
  }, { onData: part => { if (part.includes(Buffer.from('inspect_item'))) release() } })
  try {
    await timeout(received, 1500, 'frame delivery before EOF')
    finish()
    const result = await timeout(client.complete)
    assert.deepEqual(JSON.parse(decode(result.body)[0].choices[0].delta.tool_calls[0].function.arguments), JSON.parse(args))
  } finally { finish?.(); client.stop() }
})

test('guard negotiates identity and refuses compressed SSE without forwarding corrupt bytes', async () => {
  let advertised
  const result = await exchange((req, res) => {
    advertised = req.headers['accept-encoding']
    res.writeHead(200, { 'content-type': 'text/event-stream', 'content-encoding': 'gzip' })
    res.end(gzipSync(storm()))
  })
  assert.equal(advertised, 'identity')
  assert.equal(result.status, 502)
  assert.equal(result.headers['content-encoding'], undefined)
  assert.match(JSON.parse(result.body).error.message, /identity/)
})

test('cap-disabled compressed SSE remains byte-identical', async () => {
  const body = gzipSync(storm())
  const result = await exchange((req, res) => {
    assert.equal(req.headers['accept-encoding'], 'gzip, br')
    res.writeHead(200, { 'content-type': 'text/event-stream', 'content-encoding': 'gzip', 'content-length': body.length })
    res.end(body)
  }, { route: 'off' })
  assert.equal(result.headers['content-encoding'], 'gzip')
  assert.deepEqual(result.body, body)
})

test('non-SSE compressed response remains byte-identical with the cap enabled', async () => {
  const body = gzipSync(Buffer.from('{"message":"café"}'))
  const result = await exchange((req, res) => {
    res.writeHead(200, { 'content-type': 'application/json', 'content-encoding': 'gzip', 'content-length': body.length })
    res.end(body)
  })
  assert.deepEqual(result.body, body)
  assert.equal(result.headers['content-length'], String(body.length))
})

test('a cap cut does not retain an upstream Content-Length for the uncut body', async () => {
  const body = storm()
  const result = await exchange((req, res) => {
    res.writeHead(200, { 'content-type': 'text/event-stream', 'content-length': Buffer.byteLength(body) })
    res.end(body)
  })
  assert.equal(result.headers['content-length'], undefined)
  assertSix(result)
})

test('malformed JSON before any complete payload returns a clear error', async () => {
  const result = await exchange((req, res) => {
    res.writeHead(200, { 'content-type': 'text/event-stream' }); res.end('data: {broken}\n\n')
  })
  assert.equal(result.status, 502)
  assert.match(JSON.parse(result.body).error.message, /upstream SSE/)
})

test('truncated data frame is not reported as a successful completion', async () => {
  const result = await exchange((req, res) => {
    res.writeHead(200, { 'content-type': 'text/event-stream' }); res.end('data: {"choices":[')
  })
  assert.equal(result.status, 502)
})

test('malformed data after a valid frame aborts the stream without a synthetic success', async () => {
  let end
  const client = request((req, res) => {
    res.writeHead(200, { 'content-type': 'text/event-stream' })
    res.write(encodeFrame(chunk(0)))
    end = () => res.end('data: invalid-json\n\n')
  }, { onData: () => end?.() })
  try { await assert.rejects(timeout(client.complete), /aborted|ECONNRESET|socket hang up/) }
  finally { end?.(); client.stop() }
})

test('unfinished oversized event is rejected rather than buffering the response indefinitely', async () => {
  const result = await exchange((req, res) => {
    res.writeHead(200, { 'content-type': 'text/event-stream' })
    res.end('data: ' + 'x'.repeat(8 * 1024 * 1024))
  })
  assert.equal(result.status, 502)
})

test('client cancellation closes a still-open upstream stream', async () => {
  let close
  const closed = new Promise(resolve => { close = resolve })
  let received
  const first = new Promise(resolve => { received = resolve })
  const client = request((req, res) => {
    res.once('close', close)
    res.writeHead(200, { 'content-type': 'text/event-stream' }); res.write(encodeFrame(chunk(0), 'crlf'))
  }, { onData: received })
  try {
    await timeout(first)
    client.stop()
    await timeout(closed, 1500, 'upstream cancellation')
  } finally { client.stop() }
})

test('slow reader backpressures an endless upstream; cancellation releases the blocked relay', async () => {
  let sent = 0; let close; let started
  const closed = new Promise(resolve => { close = resolve })
  const ready = new Promise(resolve => { started = resolve })
  const event = encodeFrame(JSON.stringify({ choices: [{ delta: { content: 'x'.repeat(32768) } }] }))
  const client = request((req, res) => {
    res.once('close', close)
    res.writeHead(200, { 'content-type': 'text/event-stream' })
    const pump = () => {
      while (!res.destroyed && sent < 64 * 1024 * 1024) {
        sent += Buffer.byteLength(event)
        if (!res.write(event)) { res.once('drain', pump); started(); return }
      }
      started()
    }
    pump()
  }, { paused: true })
  try {
    await timeout(ready)
    await new Promise(resolve => setTimeout(resolve, 300))
    assert.ok(sent < 64 * 1024 * 1024, `upstream sent entire 64 MiB despite paused client: ${sent}`)
    client.stop()
    await timeout(closed, 1500, 'backpressured cancellation')
  } finally { client.stop() }
})
