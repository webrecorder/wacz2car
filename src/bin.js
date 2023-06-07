#!/usr/bin/env node
import { openAsBlob, createWriteStream } from 'node:fs'
import { open } from 'node:fs/promises'
import { parseArgs } from 'node:util'
import { pathToFileURL, fileURLToPath } from 'node:url'
import { Writable } from 'node:stream'

import { createLoader } from '@webrecorder/wabac/src/blockloaders.js'
import { CarWriter } from '@ipld/car/writer'

import { wacz2Car } from './index.js'

const USAGE_NOTES = `Usage:

wacz2car --input archive.wacz --output archive.car

You can also use -i and -o shortforms.`

const { values: args } = parseArgs({
  options: {
    input: {
      type: 'string',
      short: 'i'
    },
    output: {
      type: 'string',
      short: 'o',
      default: 'archive.car'
    },
    help: {
      type: 'boolean',
      short: 'h'
    }
  }
})

const { input, output, help } = args

if (help) {
  console.log(USAGE_NOTES)
  process.exit(0)
}

if (!input || !output) {
  console.log(USAGE_NOTES)
  process.exit(1)
}

let inputURL = input

if (!inputURL.startsWith('http://') && !inputURL.startsWith('https://')) {
  inputURL = pathToFileURL(inputURL).href
}

if (inputURL.startsWith('file:')) {
  const filePath = fileURLToPath(inputURL)
  const blob = await openAsBlob(filePath)
  inputURL = URL.createObjectURL(blob)
}

const loader = await createLoader({ url: inputURL })

const outputStream = createWriteStream(output)

const stream = wacz2Car(loader)

await stream.readable.pipeTo(Writable.toWeb(outputStream))

const rootCID = stream.finalBlock.cid

const outputFileDescriptor = await open(output, 'r+')

await CarWriter.updateRootsInFile(outputFileDescriptor, [rootCID])

await outputFileDescriptor.close()

console.log(`ipfs://${rootCID}`)
