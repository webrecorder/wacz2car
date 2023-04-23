import test from 'ava'
import { openAsBlob } from 'node:fs'

import { CarBufferReader } from '@ipld/car'
import { recursive as exporter } from 'ipfs-unixfs-exporter'

import { wacz2Car } from './src/index.js'

test('Convert example to a CAR', async (t) => {
  const exampleBlob = await openAsBlob('fixtures/example.wacz')

  const fullBuffer = await exampleBlob.arrayBuffer()

  const stream = wacz2Car(exampleBlob)

  const carChunks = []

  for await (const carChunk of stream.readable) {
    carChunks.push(carChunk)
  }

  const fileRoot = stream.finalBlock.cid

  const carBuffer = Buffer.concat(carChunks)

  t.pass('Successfully generated CAR file')

  const reader = CarBufferReader.fromBytes(carBuffer)

  t.pass('Generated CAR file can be parsed')

  const entries = exporter(fileRoot, {
    async get (cid) {
      const block = await reader.get(cid)
      return block.bytes
    }
  })

  const [file] = await collect(entries)

  t.assert(file, 'got file back out of CAR')

  console.log(file.node.Links)

  const fileChunks = await collect(file.content())
  const fileBuffer = Buffer.concat(fileChunks).buffer

  const actual = new Uint8Array(fileBuffer);
  const expected = new Uint8Array(fullBuffer);

  // check sizes first
  t.is(actual.length, expected.length);

  // check data
  t.deepEqual(actual, expected, 'Resulting WACZ is same as input')
})

async function collect (iterator) {
  const list = []
  for await (const item of iterator) {
    list.push(item)
  }
  return list
}
