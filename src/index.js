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

// Blocks smaller than or equal to this size will be inlined into a CID
const BLOCK_INLINE_SIZE = 32

const DIRECTORY_SIGNATURE = 0x02014b50
const LOCAL_FILE_HEADER_SIGNATURE = 0x04034b50
const CENTRAL_DIRECTORY_FILE_SIGNATURE = 0x02014b50
const EOCD_SIGNATURE = 0x06054b50
const EOCD_SIGNATURE_BYTE_LENGTH = 4

const utf8decoder = new TextDecoder()

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
 * @param {Blob} waczBlob
 * @returns {CAREncoderStream}
 */
export function wacz2Car (waczBlob) {
  const blockStream = wacz2BlockStream(waczBlob)

  const carEncoder = new CAREncoderStream([])

  blockStream.pipeThrough(carEncoder)

  return carEncoder
}

/*
 * @param {Blob} waczBlob
 * @returns {ReadableStream<Block>}
*/
export function wacz2BlockStream (waczBlob) {
  const { readable, writable } = new TransformStream(
    {},
    withCapacity(1048576 * 32)
  )

  const writer = createWriter({ writable })

  wacz2Writer(waczBlob, writer).then(async () => {
    await writer.writer.ready
    await writer.close()
  }).catch((reason) => {
    console.error(reason.stack)
    writable.abort(reason.stack)
  })

  return readable
}

/*
 * @param {Blob} waczBlob
 * @param {BlockWriter} writer
 * @returns {Promise<CID>}
*/
export async function wacz2Writer (waczBlob, writer) {
  // TODO: Convert to a class for passing in options

  const waczRoot = newNode()
  for await (const { data, name } of splitZip(waczBlob)) {
    const isWarcFile = name.endsWith('.warc')
    if (isWarcFile) {
      const cid = await warc2Writer(data, writer)
      concatCID(waczRoot, cid, data.size)
    } else {
      await concatBlob(waczRoot, data, writer, name)
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
export async function warc2Writer (warcBlob, writer) {
  const warcRoot = newNode()

  for await (const entry of splitWarc(warcBlob)) {
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
async function * splitZip (zipBlob) {
  const eocdSignatureOffset = await findEOCDStart(zipBlob)

  // EOCD is at the end of the buffer, so lets read the whole thing
  // Hopefully the "comment" and the rest of the data isn't too huge?
  const eocdBuffer = await zipBlob.slice(eocdSignatureOffset).arrayBuffer()
  const eocdView = new DataView(eocdBuffer)

  const eocdStart = eocdView.getUint32(16, true)

  const entryOffsets = []

  let entryOffset = eocdStart

  // Build list, then sort by early first
  while (entryOffset < eocdSignatureOffset) {
    const chunk = zipBlob.slice(entryOffset)
    const zipEntryOffset = await getEntryOffset(chunk)
    entryOffsets.push(zipEntryOffset)
    entryOffset += zipEntryOffset.next
  }

  entryOffsets.sort((a, b) => a.start - b.start)

  let readOffset = 0

  for (const entryInfo of entryOffsets) {
    const { start, size, name } = entryInfo
    // no need for this as separate blob, small blob
    // can just start next chunk from 'readOffset'
    //if (start !== readOffset) {
    //  const rawBlob = slice(zipBlob, readOffset, start)

    //  yield { data: rawBlob, name: '' }
    //}
    const headerSize = await getZipEntryHeaderSize(zipBlob.slice(start))
    const fileStart = start + headerSize
    //const headerBlob = slice(zipBlob, start, fileStart)
    const headerBlob = slice(zipBlob, readOffset, fileStart)
    yield { data: headerBlob, name: '' }

    const fileEnd = fileStart + size
    const fileBlob = slice(zipBlob, fileStart, fileEnd)
    yield { data: fileBlob, name }

    readOffset = fileEnd
  }

  const finalBlob = zipBlob.slice(readOffset)

  yield { data: finalBlob, name: '' }
}

/*
 * @param {Blob} entryBlob
 * @returns {Promise<ZipEntryOffset>}
*/
async function getEntryOffset (entryBlob) {
  const fileHeaderBuffer = await entryBlob.slice(0, 46).arrayBuffer()
  const fileHeaderView = new DataView(fileHeaderBuffer)

  const signature = fileHeaderView.getUint32(0, true)
  if (signature !== CENTRAL_DIRECTORY_FILE_SIGNATURE) {
    throw new Error(`Got invalid signature for central directory file header. Got ${signature.toString(16)}, expected ${CENTRAL_DIRECTORY_FILE_SIGNATURE.toString(16)}`)
  }

  const compressionMethod = fileHeaderView.getUint16(10, true)

  if (compressionMethod !== 0) {
    throw new Error(`Unable to support compressed ZIP files. Got compression ${compressionMethod}, expected 0`)
  }

  const fileUncompressedSize = fileHeaderView.getUint32(24, true)
  const fileNameLength = fileHeaderView.getUint16(28, true)
  const fileHeaderStart = fileHeaderView.getUint32(42, true)
  const extraFieldLength = fileHeaderView.getUint16(30, true)
  const fileCommentLength = fileHeaderView.getUint16(32, true)

  let fileNameBuffer = await slice(entryBlob, 46, 46 + fileNameLength).arrayBuffer()

  if (fileNameBuffer.byteLength > fileNameLength) {
    fileNameBuffer = fileNameBuffer.slice(0, fileNameLength)
  }

  const name = utf8decoder.decode(fileNameBuffer)
  const start = fileHeaderStart
  const size = fileUncompressedSize

  const next = 46 + fileNameLength + extraFieldLength + fileCommentLength

  return { start, size, name, next }
}

/*
 * @param {Blob} zipBlob
 * @returns {Promise<Number>}
*/
async function findEOCDStart (zipBlob) {
  // Start reading from the end assuming there's no comment
  let startByte = zipBlob.size - 22

  // Keep searching until we reach the beginning
  while (startByte >= 0) {
    const endByte = startByte + EOCD_SIGNATURE_BYTE_LENGTH
    const buffer = await slice(zipBlob, startByte, endByte).arrayBuffer()
    const view = new DataView(buffer)
    const signature = view.getUint32(0, true)

    if (signature === EOCD_SIGNATURE) {
      return startByte
    }

    startByte--
  }

  throw new Error('Unable to find EOCD signature in ZIP file. Check to see if it is corrupted.')
}

/*
 * @param {Blob} zipBlob
 * @returns {Promise<Number>}
*/
async function getZipEntryHeaderSize (zipBlob) {
  // Parse out the header info and where it ends

  const headerBuffer = await zipBlob.slice(0, 30).arrayBuffer()
  const headerView = new DataView(headerBuffer)

  // TODO: Account for "data descriptors"?
  const signature = headerView.getUint32(0, true)

  if (signature !== LOCAL_FILE_HEADER_SIGNATURE) {
    throw new Error(`Unrecognized ZIP header signature ${signature.toString(16)}, expected ${LOCAL_FILE_HEADER_SIGNATURE.toString(16)}`)
  }

  const compressionMethod = headerView.getUint16(8, true)
  // TODO: Better message with what to do about it?
  if (compressionMethod !== 0) throw new Error(`Unable to process compressed ZIP files. Expected compression '0x00', got ${compressionMethod.toString(16)}`)

  // TODO: This is the "uncompressed" size. Should we also check agains the compressed size?
  // Some ZIPs don't have a size
  const nameLength = headerView.getUint16(26, true)
  const extraLength = headerView.getUint16(28, true)
  const headerLength = 30 + nameLength + extraLength

  return headerLength
}

/*
 * @param {Blob} zipBlob
 * @returns {Promise<ZipEntry>}
*/
async function getZipEntry (zipBlob) {
  const headerBuffer = await zipBlob.slice(0, 30).arrayBuffer()
  const headerView = new DataView(headerBuffer)

  // TODO: Account for "data descriptors"?
  const signature = headerView.getUint32(0, true)

  if (signature !== LOCAL_FILE_HEADER_SIGNATURE) {
    throw new Error(`Unrecognized ZIP header signature ${signature.toString(16)}`)
  }

  const compressionMethod = headerView.getUint16(8, true)
  // TODO: Better message with what to do about it?
  if (compressionMethod !== 0) throw new Error(`Unable to process compressed ZIP files. Expected compression '0x00', got ${compressionMethod.toString(16)}`)

  // TODO: This is the "uncompressed" size. Should we also check agains the compressed size?
  const nameLength = headerView.getUint16(26, true)
  const extraLength = headerView.getUint16(28, true)
  const headerLength = 30 + nameLength + extraLength

  // For some reason this is yielding a Blob of size 77 instead of a blob of size nameLength
  const nameBuffer = await slice(zipBlob, 30, 30 + nameLength).arrayBuffer()
  const name = utf8decoder.decode(nameBuffer)

  const fullHeaderBlob = zipBlob.slice(0, headerLength)

  const entry = {
    data: fullHeaderBlob,
    name
  }

  return entry
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
async function concatBlob (file, blob, writer, name) {
  // Detect size
  const size = blob.size
  // disabled for now
  if (false && size <= BLOCK_INLINE_SIZE) {
    // TODO: Make inline CID instead of block encoding of the data
  } else {
    // TODO: Pass chunking options here
    const fileWriter = createFileWriter(writer)

    let actualSize = 0;

    const stream = blob.stream()
    const reader = await stream.getReader()

    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      actualSize += value.length;
      fileWriter.write(value)
    }

    console.log("actualSize", "size", actualSize, size, name ? name : "<zip data>");

    const { cid } = await fileWriter.close()

    concatCID(file, cid, blob.size)
  }
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
function slice(blob, start, end) {
  return blob.slice(start, end).slice(0, end - start);
}
