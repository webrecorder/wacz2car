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

  const writer = createWriter({
    writable,

    settings: {
      // Encode leaf nodes as raw blocks
      //fileChunkEncoder: Raw,
      //smallFileEncoder: Raw
    }
  })

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

  const iterator = parser[Symbol.asyncIterator]()

  while (true) {
    const { value: record, done } = await iterator.next()
    if (done) break
    const type = record.warcType
    const warcHeadersEndOffset = parser._warcHeadersLength

    await record.skipFully()

    if (type === 'response') {
      const reqResPair = newNode()
      const recordStart = offset + parser.offset
      const recordLength = parser.recordLength

      const warcHeadersLength = warcHeadersEndOffset - parser.offset

      const contentStart = recordStart + warcHeadersLength
      const contentLength = recordLength - warcHeadersLength

      console.log({ record, headers: [...record.warcHeaders.headers], recordStart, recordLength, contentStart, contentLength, warcHeadersLength })

      const headersStream = await loader.getRange(recordStart, warcHeadersLength, true)
      await concatStream(reqResPair, headersStream, writer)

      const contentStream = await loader.getRange(contentStart, contentLength, true)
      await concatStream(reqResPair, contentStream, writer)

      const { value: request, done } = await iterator.next()

      if (done) {
        const totalLength = recordLength + WARC_RECORD_END_BYTES.length
        concatCID(reqResPair, WARC_RECORD_END, WARC_RECORD_END_BYTES.length)
        const cid = await putUnixFS(reqResPair, writer)
        concatCID(warcRoot, cid, totalLength)
        continue
      }

      const requestType = request.warcType

      if (requestType !== 'request') {
        throw new Error(`Unable to parse WARC, expected 'request' after 'response', got ${requestType}`)
      }

      await record.skipFully()

      // Include newlines before and after the request record
      const requestStart = offset + parser.offset - WARC_RECORD_END_BYTES.length
      const requestLength = parser.recordLength + WARC_RECORD_END_BYTES.length * 2

      const requestStream = await loader.getRange(requestStart, requestLength, true)
      await concatStream(reqResPair, requestStream, writer)

      const totalLength = recordLength + WARC_RECORD_END_BYTES.length + requestLength

      const cid = await putUnixFS(reqResPair, writer)
      concatCID(warcRoot, cid, totalLength)

      continue
    }

    const recordStart = offset + parser.offset
    // Since we're reading the full record, include the end newlines in this chunk
    const recordLength = parser.recordLength + WARC_RECORD_END_BYTES.length

    const recordStream = await loader.getRange(recordStart, recordLength, true)
    await concatStream(warcRoot, recordStream, writer)
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
 * @param {ReadableStream} stream
 * @param {BlockWriter} writer
 * @returns {Promise<CID>}
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

  return cid
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
