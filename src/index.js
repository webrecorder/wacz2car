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

import * as PB from '@ipld/dag-pb'
import { CID } from 'multiformats/cid'
import { sha256 } from 'multiformats/hashes/sha2'

import { ZipRangeReader } from '@webrecorder/wabac/src/wacz/ziprangereader.js'

// Blocks smaller than or equal to this size will be inlined into a CID
// const BLOCK_INLINE_SIZE = 32

/*
export interface ChunkSettings {
  rawLeaves: boolean
  maxSize: number
}

const DEFAULT_CHUNK_SETTINGS = {
  rawLeaves: true,
  maxSize: 256
}

interface Link {
  Name: string
  TSize: number
  Hash: CID
}

interface UnixFSInProgress {
  node: UnixFS
  links: Link[]
}

interface ZipEntry {
  data: Blob
  name: string
}

interface ZipEntryOffset {
  start: number
  size: number
  name: string
  next: number
}

interface WarcRecord {
  blob: Blob
  bodyOffset: number
  // TODO: Parsed header info?
}
*/

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

  for await (const { data, name } of splitZip(zipreader)) {
    const isWarcFile = name.endsWith('.warc')
    if (isWarcFile) {
      const cid = await warc2Writer(data, writer)
      concatCID(waczRoot, cid, data.size)
    } else {
      await concatStream(waczRoot, data, writer, name)
    }
  }

  const cid = await putUnixFS(waczRoot, writer)

  return cid
}

/*
 * @param {Blob} warcBlob
 * @param {BlockWriter} writer
 * @returns {Promise<CID>}
*/
export async function warc2Writer (loader, offset, length, writer) {
  const warcRoot = newNode()

  for await (const entry of splitWarc(loader, offset, length)) {
    // TODO: Extract response body into own subfile
    await concatBlob(warcRoot, entry.blob, writer)
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
      uncompressedSize,
      offset,
      localEntryOffset
    } = entryInfo

    const end = offset + compressedSize
    let start = localEntryOffset

    if (localEntryOffset > readOffset) {
      start = readOffset
    }

    // TODO: Handle signals
    const headerChunk = await zipReader.loader.getRange(start, offset - start, true)

    yield { data: headerChunk, name: '', compressed: false }

    const dataChunk = await zipReader.loader.getRange(offset, compressedSize, true)

    yield { data: dataChunk, name: filename, compressed: !!deflate }

    readOffset = end
  }

  const length = await zipReader.loader.getLength()

  if (readOffset < length) {
    const finalData = await zipReader.loader.getRange(readOffset, length - readOffset, true)

    yield { data: finalData, name: '', compressed: false }
  }
}

/*
 * @param {Blob} warcBlob
 * @returns {AsyncIterator<WarcRecord>}
*/
async function * splitWarc (warcBlob) {
  // Stat reading warc headings
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
async function concatStream (file, stream, writer, name) {
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
 * @param {Blob} blob
 * @param {BlockWriter} writer
 * @returns {Promise<void>}
*/
async function concatBlob (file, blob, writer, name) {
  // Detect size
  const size = blob.size
  // disabled for now
  // if (false && size <= BLOCK_INLINE_SIZE) {
  // TODO: Make inline CID instead of block encoding of the data
  // } else {
  // TODO: Pass chunking options here
  const fileWriter = createFileWriter(writer)

  let actualSize = 0

  const stream = blob.stream()
  const reader = await stream.getReader()

  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    actualSize += value.length
    fileWriter.write(value)
  }

  const { cid } = await fileWriter.close()

  concatCID(file, cid, blob.size)
  // }
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

// for now, as slice() returns incorrect length if offset is not 0!
function slice (blob, start, end) {
  return blob.slice(start, end).slice(0, end - start)
}
