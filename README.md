# wacz2car
Conver WACZ files to CAR files for uploading to IPFS.

```JavaScript
import {wacz2Car} from 'wacz2car'
import { createLoader } from '@webrecorder/wabac/src/blockloaders.js'

const loader = await createLoader({someURL})
const carStream = wacz2Car(loader)

for await (const chunk of stream.readable) {
  writeChunkToFile(chunk)
}

const rootCID = stream.finalBlock.cid
```
