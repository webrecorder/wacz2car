declare module 'node:fs' {
  export function openAsBlob(path: string) : Promise<Blob>
}


interface ReadableStream<Uint8Array> {
  [Symbol.asyncIterator]() : AsyncIterator<Uint8Array>
}
