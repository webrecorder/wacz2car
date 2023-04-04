import test from 'ava'
import { openAsBlob } from 'node:fs'

import { CarReader, CarWriter } from '@ipld/car'


import {wacz2Car} from './src/index'

test('Convert example to a CAR', async (t) => {
  const exampleBlob = await openAsBlob('fixtures/example.wacz')

  const {done, stream} = wacz2Car(exampleBlob)

  const reader = await CarReader.fromIterable(stream)

  const cid = await done

  // TODO read from reader
})
