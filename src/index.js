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
// import { UnixFS } from 'ipfs-unixfs'
import * as UnixFS from '@ipld/unixfs'
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
      // fileChunkEncoder: Raw,
      // smallFileEncoder: Raw
    }
  })

  wacz2Writer2(blockLoader, writer).then(async () => {
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
    // const warcHeadersEndOffset = parser._warcHeadersLength

    // await record.skipFully()

    if (type === 'response') {
      const reqResPair = newNode()
      const recordStart = parser.offset

      const contentStart = parser._reader.getReadOffset()
      const headersLength = contentStart - parser.offset

      const headersStream = await loader.getRange(recordStart + offset, headersLength, true)
      await concatStream(reqResPair, headersStream, writer)

      await record.skipFully()

      const recordLength = parser.recordLength
      const contentLength = recordLength - headersLength

      const contentStream = await loader.getRange(contentStart + offset, contentLength, true)
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

      // start from end of content
      const requestStart = contentStart + contentLength

      // include whatever is left from previous record (newlines) until end of request record
      const requestLength = parser.recordLength + (parser.offset - requestStart) + WARC_RECORD_END_BYTES.length

      const requestStream = await loader.getRange(requestStart + offset, requestLength, true)
      await concatStream(reqResPair, requestStream, writer)

      const totalLength = recordLength + 2 * WARC_RECORD_END_BYTES.length + requestLength

      const cid = await putUnixFS(reqResPair, writer)
      // console.log("PAIR CID", cid, reqResPair);

      concatCID(warcRoot, cid, totalLength)

      continue
    } else {
      await record.skipFully()

      // Since we're reading the full record, include the end newlines in this chunk
      const recordLength = parser.recordLength + WARC_RECORD_END_BYTES.length
      const recordStream = await loader.getRange(parser.offset + offset, recordLength, true)

      const recordNode = newNode()
      await concatStream(recordNode, recordStream, writer)

      const cid = await putUnixFS(recordNode, writer)
      // console.log("SINGLE CID", cid, recordNode);
      concatCID(warcRoot, cid, recordLength)
    }
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


// ======== from awp-sw ==========

async function splitByWarcRecordGroup (writer, warcIter, markers) {
  let links = []
  const fileLinks = []
  let secondaryLinks = []

  let inZipFile = false
  let lastChunk = null
  let currName = null

  const decoder = new TextDecoder()

  const dirs = {}

  const { ZIP, WARC_PAYLOAD, WARC_GROUP } = markers

  let file = UnixFS.createFileWriter(writer)

  function getDirAndName (fullpath) {
    const parts = fullpath.split('/')
    const filename = parts.pop()
    return [parts.join('/'), filename]
  }

  const waczDir = UnixFS.createDirectoryWriter(writer)

  let count = 0

  for await (const chunkData of warcIter) {
    let chunk;
    let name = null;
    if (chunkData instanceof Array) {
      chunk = chunkData[0];
      name = chunkData[1];
    } else {
      chunk = chunkData;
    }
    if (chunk === ZIP && !inZipFile) {
      if (name) {
        currName = decoder.decode(name);
      }
      // if (lastChunk) {
      //   currName = decoder.decode(lastChunk)
      // }
      inZipFile = true

      if (count) {
        fileLinks.push(await file.close())
        count = 0
        file = UnixFS.createFileWriter(writer)
      }
    } else if (chunk === ZIP && inZipFile) {
      if (count) {
        links.push(await file.close())
        count = 0
        file = UnixFS.createFileWriter(writer)
      }

      let link

      if (secondaryLinks.length) {
        if (links.length) {
          throw new Error('invalid state, secondaryLinks + links?')
        }
        link = await concat(writer, secondaryLinks)
        secondaryLinks = []
      } else {
        link = await concat(writer, links)
        links = []
      }

      fileLinks.push(link)

      const [dirName, filename] = getDirAndName(currName)
      currName = null

      let dir

      if (!dirName) {
        dir = waczDir
      } else {
        if (!dirs[dirName]) {
          dirs[dirName] = UnixFS.createDirectoryWriter(writer)
        }
        dir = dirs[dirName]
      }

      dir.set(filename, link)

      inZipFile = false
    } else if (chunk === WARC_PAYLOAD || chunk === WARC_GROUP) {
      if (!inZipFile) {
        throw new Error('invalid state')
      }

      if (count) {
        links.push(await file.close())
        count = 0
        file = UnixFS.createFileWriter(writer)

        if (chunk === WARC_GROUP) {
          secondaryLinks.push(await concat(writer, links))
          links = []
        }
      }
    } else if (chunk.length > 0) {
      // if (!inZipFile) {
      //   lastChunk = chunk
      // }
      file.write(chunk)
      count++
    }
  }

  fileLinks.push(await file.close())

  for (const [name, dir] of Object.entries(dirs)) {
    waczDir.set(name, await dir.close())
  }

  const waczDirRoot = await waczDir.close()
  const waczFileRoot = await concat(writer, fileLinks)

  console.log("wacz dir root", waczDirRoot);
  console.log("wacz file root", waczFileRoot);

  //rootDir.set('webarchive', waczDirRoot)
  //rootDir.set(waczPath, waczFileRoot)
}

async function concat (writer, links) {
  // TODO: is this the right way to do this?
  const { fileEncoder, hasher, linker } = writer.settings
  const advanced = fileEncoder.createAdvancedFile(links)
  const bytes = fileEncoder.encode(advanced)
  const hash = await hasher.digest(bytes)
  const cid = linker.createLink(fileEncoder.code, hash)
  const block = { bytes, cid }
  writer.writer.write(block)

  const link = {
    cid,
    contentByteLength: fileEncoder.cumulativeContentByteLength(links),
    dagByteLength: fileEncoder.cumulativeDagByteLength(bytes, links)
  }

  return link
}

export async function * iterWacz (loader, markers) {
  const zipreader = new ZipRangeReader(loader)

  let first = true;

  for await (const { name, start, length } of splitZip(zipreader)) {
    if (first) {
      first = false;
    } else if (name) {
      yield [markers.ZIP, new TextEncoder().encode(name)];
    } else {
      yield markers.ZIP
    }

    const isWarcFile = name.endsWith('.warc')
    if (isWarcFile) {
      for await (const data of splitWarc(loader, start, length, markers)) {
        yield data
      }
    } else {
      const reader = await loader.getRange(start, length, true)
      for await (const data of iterate(reader)) {
        yield data
      }
    }
  }
}

export const iterate = async function * (stream) {
  const reader = stream.getReader()
  while (true) {
    const next = await reader.read()
    if (next.done) {
      return
    } else {
      yield next.value
    }
  }
}

async function wacz2Writer2 (loader, writer) {
  const ZIP = new Uint8Array([])
  const WARC_PAYLOAD = new Uint8Array([])
  const WARC_GROUP = new Uint8Array([])
  const markers = { ZIP, WARC_PAYLOAD, WARC_GROUP }

  //const rootDir = UnixFS.createDirectoryWriter(writer)

  //const waczPath = 'webarchive.wacz'

  await splitByWarcRecordGroup(writer, iterWacz(loader, markers), markers)
}

export async function * splitWarc (loader, offset, length, markers) {
  const stream = await loader.getRange(offset, length, true)

  const reader = await stream.getReader()
  const parser = new WARCParser(reader)

  const iterator = parser[Symbol.asyncIterator]()

  while (true) {
    const { value: record, done } = await iterator.next()
    if (done) break

    yield markers.WARC_GROUP

    const type = record.warcType
    // const warcHeadersEndOffset = parser._warcHeadersLength

    // await record.skipFully()

    if (type === 'response') {
      // const reqResPair = newNode()
      const recordStart = parser.offset

      const contentStart = parser._reader.getReadOffset()
      const headersLength = contentStart - parser.offset

      yield await loader.getRange(recordStart + offset, headersLength, false)

      await record.skipFully()

      const recordLength = parser.recordLength
      const contentLength = recordLength - headersLength

      const reader = await loader.getRange(contentStart + offset, contentLength, true)

      yield markers.WARC_PAYLOAD

      for await (const data of iterate(reader)) {
        yield data
      }

      yield markers.WARC_PAYLOAD

      const { value: request, done } = await iterator.next()

      if (done) {
        // const totalLength = recordLength + WARC_RECORD_END_BYTES.length
        // concatCID(reqResPair, WARC_RECORD_END, WARC_RECORD_END_BYTES.length)
        // const cid = await putUnixFS(reqResPair, writer)
        // concatCID(warcRoot, cid, totalLength)
        continue
      }

      const requestType = request.warcType

      if (requestType !== 'request') {
        throw new Error(`Unable to parse WARC, expected 'request' after 'response', got ${requestType}`)
      }

      await record.skipFully()

      // start from end of content
      const requestStart = contentStart + contentLength

      // include whatever is left from previous record (newlines) until end of request record
      const requestLength = parser.recordLength + (parser.offset - requestStart) + WARC_RECORD_END_BYTES.length

      yield await loader.getRange(requestStart + offset, requestLength, false)
      // await concatStream(reqResPair, requestStream, writer)

      // const totalLength = recordLength + 2 * WARC_RECORD_END_BYTES.length + requestLength

      // const cid = await putUnixFS(reqResPair, writer)
      // // console.log("PAIR CID", cid, reqResPair);

      // concatCID(warcRoot, cid, totalLength)

    } else {
      await record.skipFully()

      // Since we're reading the full record, include the end newlines in this chunk
      const recordLength = parser.recordLength + WARC_RECORD_END_BYTES.length
      yield await loader.getRange(parser.offset + offset, recordLength)

      // const recordNode = newNode()
      // await concatStream(recordNode, recordStream, writer)

      // const cid = await putUnixFS(recordNode, writer)
      // // console.log("SINGLE CID", cid, recordNode);
      // concatCID(warcRoot, cid, recordLength)
    }
  }

  yield markers.WARC_GROUP

  // const cid = await putUnixFS(warcRoot, writer)

  // return cid
}
