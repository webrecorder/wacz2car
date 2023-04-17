import test from 'ava'
import { openAsBlob } from 'node:fs'

import {CarBufferReader} from '@ipld/car'
import { recursive as exporter } from 'ipfs-unixfs-exporter'

import {wacz2Car} from './src/index.js'

test('Convert example to a CAR', async (t) => {
  const exampleBlob = await openAsBlob('fixtures/example.wacz')

  const stream = wacz2Car(exampleBlob)

  const carChunks = []

  for await (const carChunk of stream) {
    console.log('chunk', carChunk.length)
    carChunks.push(carChunk)
  }

  const carBuffer = Buffer.from(carChunks)

  const reader = CarBufferReader.fromBytes(carBuffer)

  const entries = exporter(roots[0], {
  async get (cid) {
    const block = await reader.get(cid)
    return block.bytes
  }
})

for await (const entry of entries) {
  if (entry.type === 'file' || entry.type === 'raw') {
    console.log('file', entry.path, entry.content)
  } else if (entry.type === 'directory') {
    console.log('directory', entry.path)
  }
}
})
