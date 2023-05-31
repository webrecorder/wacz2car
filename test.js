import test from 'ava'
import { openAsBlob } from 'node:fs'
import { writeFile } from 'node:fs/promises'

import { CarBufferReader } from '@ipld/car'
import { recursive as exporter } from 'ipfs-unixfs-exporter'
import { identity as Identity } from 'multiformats/hashes/identity'

import { createLoader } from '@webrecorder/wabac/src/blockloaders.js'

import { wacz2Car } from './src/index.js'

test('Convert example to a CAR', async (t) => {
//  const exampleBlob = await openAsBlob('fixtures/webarchive-duplicates.wacz')
  const exampleBlob = await openAsBlob('fixtures/example.wacz')
  const url = URL.createObjectURL(exampleBlob)

  const loader = await createLoader({ url })

  const fullBuffer = await exampleBlob.arrayBuffer()

  const stream = wacz2Car(loader)

  const carChunks = []

  for await (const carChunk of stream.readable) {
    carChunks.push(carChunk)
  }

  const fileRoot = stream.finalBlock.cid

  const carBuffer = Buffer.concat(carChunks)

  await writeFile('fixtures/example.car', carBuffer)

  t.pass('Successfully generated CAR file')

  const reader = CarBufferReader.fromBytes(carBuffer)

  t.pass('Generated CAR file can be parsed')

  const entries = exporter(fileRoot, {
    async get (cid) {
      if (cid.multihash.code === Identity.code) {
        return cid.multihash.digest
      }
      const block = await reader.get(cid)

      return block.bytes
    }
  })

  const [file] = await collect(entries)

  t.assert(file, 'got file back out of CAR')

  const fileChunks = await collect(file.content())
  const fileBuffer = Buffer.concat(fileChunks).buffer

  const actual = new Uint8Array(fileBuffer)
  const expected = new Uint8Array(fullBuffer)

  // check sizes first
  t.is(actual.length, expected.length)

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
