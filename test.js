import test from 'ava'
import { openAsBlob } from 'node:fs'

import { CarReader, CarWriter } from '@ipld/car'

import {wacz2Car} from './src/index.js'

test('Convert example to a CAR', async (t) => {
  const exampleBlob = await openAsBlob('fixtures/example.wacz')

  const stream = wacz2Car(exampleBlob)

  console.log(stream)

  const carChunks = []

  for await (const carChunk of stream) {
    console.log({carChunk})
    carChunks.push(carChunk)
  }

  const carBuffer = Buffer.from(carChunks)

  // TODO: Do something with chunks?
})
