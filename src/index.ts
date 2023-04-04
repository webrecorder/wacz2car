import { CID } from 'multiformats/cid'
import { Block } from 'multiformats/block'
import {
  createWriter,
  createFileWriter,
  withCapacity,
  BlockWriter
} from '@ipld/unixfs'
import { UnixFS } from 'ipfs-unixfs'
import { CAREncoderStream } from 'ipfs-car'
import * as PB from '@ipld/dag-pb'

// Blocks smaller than or equal to this size will be inlined into a CID
const BLOCK_INLINE_SIZE = 32

const DIRECTORY_SIGNATURE = 0x02014b50

const utf8decoder = new TextDecoder()

export interface ChunkSettings {
  rawLeaves: boolean
  maxSize: number
}

const DEFAULT_CHUNK_SETTINGS: ChunkSettings = {
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

export async function wacz2Car (waczBlob: Blob): { done: Promise<CID>, stream: ReadableStream<Uint8Array> } {
  const { done, readable } = wacz2BlockStream

  const carStream = new CAREncoderStream()

  const stream = readable.pipeThrough(carStream)

  return { done, stream }
}

export function wacz2BlockStream (waczBlob: Blob): { done: Promise<CID>, readable: ReadableStream<Block> } {
  const { readable, writable } = new TransformStream(
    {},
    withCapacity(1048576 * 32)
  )

  const writer = createWriter({ writable })

  const done = wacz2CarStream(waczBlob, writer).then((cid) => {
    await writable.close()
    return cid
  })

  return { done, readable }
}

// TODO: Cancellation signal?
// TODO: Convert to a class for passing in options
export async function wacz2Writer (waczBlob: Blob, writer: BlockWriter): Promise<CID> {
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

export async function warc2Writer (warcBlob: Blob, writer: BlockWriter): Promise<CID> {
  const warcRoot = newNode()

  for await (const entry of splitWarc(warcBlob)) {
    // TODO: Extract response body into own subfile
    await concatBlob(warcRoot, entry.blob, writer)
  }

  const cid = await putUnixFS(warcRoot, writer)

  return cid
}

async function * splitZip (zipBlob: Blob): AsyncIterator<ZipEntry> {
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

async function getZipEntry (zipBlob: Blob): Promise<ZipEntry> {
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
  }

  const compressionMethod = headerView.getUint16(8, true)
  // TODO: Better message with what to do about it?
  if (compressionMethod !== 0) throw new Error('Unable to process compressed ZIP files')

  // TODO: This is the "uncompressed" size. Should we also check agains the compressed size?
  const size = headerView.getUint32(22, true)
  const nameLength = headerView.getUint16(26, true)
  const extraLength = headerView.getUint16(28, true)
  const headerLength = 30 + nameLength + extraLength

  const nameBuffer = await zipBlob.slice(30, nameLength).arrayBuffer()
  const fileName = utf8decoder.decode(nameBuffer)

  const dataBlob = zipBlob.slice(headerLength, size)
  const headerBlob = zipBlob.slice(0, headerLength)
  const totalSize = size + headerLength

  return {
    dataBlob,
    headerBlob,
    totalSize,
    fileName,
    isCentralDirectory: false
  }
}

async function * splitWarc (warcBlob: Blob): AsyncIterator<WarcRecord> {
  // Stat reading warc headings
}

async function putUnixFS (file: UnixFSInProgress, writer: BlockWriter): CID {
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

async function concatBlob (file: UnixFSInProgress, blob: Blob, writer: BlockWriter): Promise<void> {
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

function concatCID (file: UnixFSInProgress, cid: CID, fileSize): void {
  file.node.addBlockSize(fileSize)
  file.links.push({
    Name: '',
    TSize: fileSize,
    Hash: cid
  })
}

function newNode (): UnixFSInProgress {
  return {
    node: new UnixFS({ type: 'file' }),
    links: []
  }
}
