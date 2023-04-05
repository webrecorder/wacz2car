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

// Blocks smaller than or equal to this size will be inlined into a CID
const BLOCK_INLINE_SIZE = 32

const DIRECTORY_SIGNATURE = 0x02014b50
const LOCAL_FILE_HEADER_SIGNATURE = 0x04034b50

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
  dataBlob: Blob
  headerBlob: Blob | null
  totalSize: number
  fileName: string
  isCentralDirectory: boolean
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

  const carEncoder = new CAREncoderStream()

  const carStream = blockStream.pipeThrough(carEncoder)

  return carStream
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

  wacz2Writer(waczBlob, writer).then(async (cid) => {
    await writable.close()
  }).catch((reason) => {
    console.log(reason.stack)
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
  for await (const entry of splitZip(waczBlob)) {
    const isWaczFile = entry.fileName.endsWith('.warc')
    if (isWaczFile === true) {
      if (entry.headerBlob !== null) {
        await concatBlob(waczRoot, entry.headerBlob, writer)
      }
      const cid = await warc2Writer(entry.dataBlob, writer)
      concatCID(waczRoot, cid, entry.blob.size)
    } else {
      if (entry.headerBlob !== null) {
        await concatBlob(waczRoot, entry.headerBlob, writer)
      }
      await concatBlob(waczRoot, entry.dataBlob, writer)
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
  // Start reading headers from the blob
  let offset = 0
  const maxSize = zipBlob.size
  while (offset < maxSize) {
    const chunk = zipBlob.slice(offset)
    const entry = await getZipEntry(chunk)
    yield entry
    if (entry.isCentralDirectory) break
    offset += entry.totalSize
  }
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

  if (signature === DIRECTORY_SIGNATURE) {
    // Remainder of the blob should contain the directory?
    return {
      dataBlob: zipBlob,
      headerBlob: null,
      totalSize: zipBlob.size,
      fileName: '',
      isCentralDirectory: true
    }
  } else if(signature !== LOCAL_FILE_HEADER_SIGNATURE) {
    throw new Error(`Unrecognized ZIP header signature ${signature.toString(16)}`)
  }

  const compressionMethod = headerView.getUint16(8, true)
  // TODO: Better message with what to do about it?
  if (compressionMethod !== 0) throw new Error(`Unable to process compressed ZIP files. Expected compression '0x00', got ${compressionMethod.toString(16)}`)

  // TODO: This is the "uncompressed" size. Should we also check agains the compressed size?
  // Some ZIPs don't have a size
  const size = headerView.getUint32(22, true)
  const nameLength = headerView.getUint16(26, true)
  const extraLength = headerView.getUint16(28, true)
  const headerLength = 30 + nameLength + extraLength

  // For some reason this is yielding a Blob of size 77 instead of a blob of size nameLength
  const nameBuffer = await zipBlob.slice(30, 30 + nameLength).arrayBuffer()
  const fileName = utf8decoder.decode(nameBuffer)

  const dataBlob = zipBlob.slice(headerLength, headerLength + size)
  const headerBlob = zipBlob.slice(0, headerLength)
  const totalSize = size + headerLength

  const entry = {
    dataBlob,
    headerBlob,
    totalSize,
    fileName,
    isCentralDirectory: false
  }

  console.log({
    entry,
    nameLength,
    extraLength,
    nameBuffer,
    headerLength,
    size,
    nameEnd,
  })

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
  const block = PB.encode(toEncode)

  await writer.write(block)

  return block.cid
}

/*
 * @param {UnixFSInProgress} file
 * @param {Blob} blob
 * @param {BlockWriter} writer
 * @returns {Promise<void>}
*/
async function concatBlob (file, blob, writer) {
  // Detect size
  const size = blob.size
  if (size <= BLOCK_INLINE_SIZE) {
    // TODO: Make inline CID instead of block encoding of the data
  } else {
    // TODO: Pass chunking options here
    const fileWriter = createFileWriter(writer)
    await fileWriter.write(blob)
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
  file.node.addBlockSize(fileSize)
  file.links.push({
    Name: '',
    TSize: fileSize,
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
