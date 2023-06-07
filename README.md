# wacz2car
Conver WACZ files to CAR files for uploading to IPFS.

## JS API

```JavaScript
import {wacz2Car} from 'wacz2car'
import { createLoader } from '@webrecorder/wabac/src/blockloaders.js'

const loader = await createLoader({someURL})
const carStream = wacz2Car(loader)

for await (const chunk of stream.readable) {
  writeChunkToFile(chunk)
}

// Use `CarWriter.updateRootsInFile` to update root in the header
const rootCID = stream.finalBlock.cid
```

## Command line

Make sure you have [Node.js](https://nodejs.org/) installed on your system.

```
# Install globally
npm i -g wacz2car

# Use
wacz2car --input archive.wacz --output archive.car

# Use without installing
npx wacz2car --input archive.wacz --output archive.car

```

You can also use -i and -o shortforms.

