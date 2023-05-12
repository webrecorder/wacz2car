/* global TransformStream */

/*
 * @typedef { import('multiformats/cid').CID } CID
 * @typedef { import('multiformats/block').Block } Block
 * @typedef { import('@ipld/unixfs').BlockWriter } BlockWriter
 */
import {
  createWriter,
  createFileWriter,
  withCapacity
} from '@ipld/unixfs'
import { UnixFS } from 'ipfs-unixfs'
import { CAREncoderStream } from 'ipfs-car'
import { WARCParser } from 'warcio'

import * as PB from '@ipld/dag-pb'
import { CID } from 'multiformats/cid'
import * as Raw from 'multiformats/codecs/raw'
import * as Bytes from 'multiformats/bytes'
import { identity } from 'multiformats/hashes/identity'
import { sha256 } from 'multiformats/hashes/sha2'

import { ZipRangeReader } from '@webrecorder/wabac/src/wacz/ziprangereader.js'

// Encode the WARC record end into a "raw" CID which contains the raw data within it
const WARC_RECORD_END_BYTES = Bytes.fromString('\r\n\r\n')
const WARC_RECORD_END = CID.create(1, Raw.code, identity.digest(WARC_RECORD_END_BYTES))

/*
 * @param {BlockLoader} blockLoader
 * @returns {CAREncoderStream}
 */
export function wacz2Car (blockLoader) {
  const blockStream = wacz2BlockStream(blockLoader)

  const carEncoder = new CAREncoderStream([])

  blockStream.pipeThrough(carEncoder)

  return carEncoder
}

/*
 * @param {BlockLoader} blockLoader
 * @returns {ReadableStream<Block>}
*/
export function wacz2BlockStream (blockLoader) {
  const { readable, writable } = new TransformStream(
    {},
    withCapacity(1048576 * 32)
  )

  const writer = createWriter({ writable })

  wacz2Writer(blockLoader, writer).then(async () => {
    await writer.writer.ready
    await writer.close()
  }).catch((reason) => {
    console.error(reason.stack)
    writable.abort(reason.stack)
  })

  return readable
}

/*
 * @param {BlockLoader} loader
 * @param {BlockWriter} writer
 * @returns {Promise<CID>}
*/
export async function wacz2Writer (loader, writer) {
  // TODO: Convert to a class for passing in options

  const waczRoot = newNode()

  const zipreader = new ZipRangeReader(loader)

  for await (const { name, start, length } of splitZip(zipreader)) {
    const isWarcFile = name.endsWith('.warc')
    if (isWarcFile) {
      const cid = await warc2Writer(loader, start, length, writer)
      concatCID(waczRoot, cid, length)
    } else {
      const data = await loader.getRange(start, length, true)

      await concatStream(waczRoot, data, writer)
    }
  }

  const cid = await putUnixFS(waczRoot, writer)

  return cid
}

/*
 * @param {BlockLoader} loader
 * @param {BlockWriter} writer
 * @returns {Promise<CID>}
*/
export async function warc2Writer (loader, offset, length, writer) {
  const warcRoot = newNode()

  const stream = await loader.getRange(offset, length, true)

  const reader = await stream.getReader()
  const parser = new WARCParser(reader)

  for await (const record of parser) {
    // TODO: Account for response type
    await record.skipFully()

    const recordStart = offset + parser.offset
    const recordLength = parser.recordLength

    const recordStream = await loader.getRange(recordStart, recordLength, true)
    await concatStream(warcRoot, recordStream, writer)
    concatCID(warcRoot, WARC_RECORD_END, WARC_RECORD_END_BYTES.length)
  }

  const cid = await putUnixFS(warcRoot, writer)

  return cid
}

/*
 * @param {Blob} zipBlob
 * @returns {AsyncIterator<ZipEntry>}
*/
async function * splitZip (zipReader) {
  const entries = await zipReader.load()

  const sorted = Object
    .values(entries)
    .sort((a, b) => {
      return a.offset - b.offset
    })

  let readOffset = 0

  for (const entryInfo of sorted) {
    const {
      filename,
      deflate,
      compressedSize,
      offset,
      localEntryOffset
    } = entryInfo

    const end = offset + compressedSize
    let start = localEntryOffset

    if (localEntryOffset > readOffset) {
      start = readOffset
    }

    const headerLength = offset - start
    // TODO: Handle signals

    yield {
      name: '',
      compressed: false,
      start,
      length: headerLength
    }

    yield {
      name: filename,
      compressed: !!deflate,
      start: offset,
      length: compressedSize
    }

    readOffset = end
  }

  const length = await zipReader.loader.getLength()

  if (readOffset < length) {
    const remainingLength = length - readOffset

    yield {
      name: '',
      compressed: false,
      start: readOffset,
      length: remainingLength
    }
  }
}

/*
 * @param {UnixFSInProgress} file
 * @param {BlockWriter} writer
 * @returns {CID}
*/
async function putUnixFS (file, writer) {
  const data = file.node.marshal()

  const toEncode = {
    Data: data,
    Links: file.links
  }

  // Encode block to dag-pb
  const bytes = PB.encode(toEncode)

  // TODO: Provide options for
  const hash = await sha256.digest(bytes)
  const cid = CID.create(1, PB.code, hash)

  await writer.writer.write({ cid, bytes })

  return cid
}

/*
 * @param {UnixFSInProgress} file
 * @param {Blob} blob
 * @param {BlockWriter} writer
 * @returns {Promise<void>}
*/
async function concatStream (file, stream, writer) {
  // TODO: Pass chunking options here
  const fileWriter = createFileWriter(writer)

  let actualSize = 0

  const reader = await stream.getReader()

  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    actualSize += value.length
    fileWriter.write(value)
  }

  const { cid } = await fileWriter.close()

  concatCID(file, cid, actualSize)
}

/*
 * @param {UnixFSInProgress} file
 * @param {CID} cid
 * @param {number} fileSize
 * @returns {void}
*/
function concatCID (file, cid, fileSize) {
  file.node.addBlockSize(BigInt(fileSize))
  file.links.push({
    Name: '',
    Tsize: fileSize,
    Hash: cid
  })
}

/*
 * @returns {UnixFSInProgress}
 */
function newNode () {
  return {
    node: new UnixFS({ type: 'file' }),
    links: []
  }
}
