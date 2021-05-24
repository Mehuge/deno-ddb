import { Hash } from 'https://deno.land/x/checksum@1.4.0/mod.ts';

export function hex(bytes: Uint8Array, max: number = 32) {
  let str = '';
  for (let i = 0; i < bytes.byteLength && i < max; i++) {
    str += bytes[i].toString(16);
  }
  return str;
}

interface Uint8ArrayQueue {
  chunk: Uint8Array;
  hash: string;
}

let debug = (Deno.env.get('DEBUG')||'').includes("U8PIPE:");

export default function getUint8Pipe() {
  const chunks: Uint8ArrayQueue[] = [];
  let totBytesIn = 0;
  let totBytesOut = 0;
  let totBlockedBytesOut = 0;
  let eof = false;

  /** checkHashes (debugging)
   * Ensure chunks written to this pipe, have not been altered since being
   * queued. For example, Deno.iter(<Deno.File>reader) returns the same
   * block instance, the contents of which have been over written.
  */
  function checkHashes() {
    chunks.forEach((chunk: Uint8ArrayQueue) => {
      const hash = new Hash('md5').digest(chunk.chunk).hex();
      if (hash != chunk.hash) {
        debugger;
        console.error('HASH ERROR, CHUNKS CORRUPT');
      }
    })
  }

  /** _read() interal reader
   * returns up to p.byteLength data from the pipe if data is available.
   * If no data is currently available, returns 0.
   * If eof signalled and all data has been returned returns null
  */
  function _read(p: Uint8Array, offset: number = 0) {
    let chunk;
    let block;
    let need = p.byteLength - offset;

    if (debug) checkHashes();

    while (chunks.length && chunks[0].chunk.byteLength <= need) {
      if (chunk = chunks.shift()) {
        const hash = chunk.hash;
        block = chunk.chunk;
        if (debug) console.log('de-queue chunk', block.byteLength, hash, hex(block));
        p.set(block, offset);
        totBytesOut += block.byteLength;
        offset += block.byteLength;
        need -= block.byteLength;
      };
    }

    if (need == 0) {
      return offset;    // have filled the buffer
    }

    if (chunks.length == 0) {
      if (offset > 0) {
        if (debug) console.log('_read: return partial block', offset);
        return offset; // return partial buffer
      }
      if (eof) {
        if (debug) console.log('_read: eof');
        return null;   // no more data, eof was signalled
      }
      if (debug) console.log('_read: no data available');
      return 0;   // no data available
    }

    if (debug) checkHashes();

    // If get here, chunk at head of queue is larger that what we
    // can fit in the buffer, so fill up the buffer, and leave the
    // rest of the chunk in the queue.
    block = chunks[0].chunk;
    if (debug) console.log('de-queue chunk and split', block.length, chunks[0].hash, hex(block));
    p.set(block.slice(0, need - 1), offset);
    totBytesOut += need;
    chunks[0].chunk = block.slice(need);
    block = chunks[0].chunk;
    chunks[0].hash = new Hash('md5').digest(block).hex();
    if (debug) {
      console.log('re-queue split chunk', block.length, chunks[0].hash, hex(block));
      console.log('_read: return full block', p.byteLength);
      checkHashes();
    }
    return p.byteLength;
  }

  /** readChunked(p: Uint8Array)
   *  do not use, not working
   *  smooths reading chunks from the pipe, will only return when p.byteLength
   *  bytes are available or at eof.
   *  debatable if its is more efficient than just giving chunks out as they are
  */
  async function readChunked(p: Uint8Array) {
    let bytes = await _read(p);
    while (bytes != null && bytes < p.byteLength) {
      if (eof) {
        if (debug) console.log('EOF');
        break;
      }
      // incomplete data, delay for more until eof
      await new Promise(resolve => setTimeout(resolve, 1));
      bytes = await _read(p, bytes);
    }
    if (debug) console.log('de-queued chunk', bytes, hex(p.slice(0, bytes||0)));
    if (bytes != null) totBlockedBytesOut += bytes;
    if (bytes == null) {
      console.log(totBytesIn, totBytesOut, totBlockedBytesOut);
    }
    return bytes;
  }

  /** read(p: Uint8Array)
   * returns the next available chunk from the pipe. If the next available chunk
   * is larger than p.byteLength then returns the first p.byteLength byts of the
   * chunk, and puts the remaining bytes at the head of the queue. It does not
   * ever attempt to merge chunks. If the top chunk is 1 byte and you want 100
   * you will only get 1 byte.
   * Enhancement: Fit as many chunks into the return buffer as can.
  */
  async function read(p: Uint8Array) {
    /* no data available, wait for some data */
    while (chunks.length == 0) {
      if (eof) break;
      await new Promise(resolve => setTimeout(resolve, 1));
    }

    /* get next non-zero lenght chunk */
    let chunk = chunks.shift();
    while (chunk && chunk.chunk.byteLength == 0) {
      chunk = chunks.shift();
    }

    /* provided we got a chunk (could be eof) */
    if (chunk) {
      let block = chunk.chunk;
      if (0) {
        if (block.byteLength <= p.byteLength) {
          totBytesOut += block.byteLength;
          if (debug) console.log('pull', block.byteLength);
          p.set(block, 0);
          return block.byteLength;
        }
      } else {
        let offset = 0;
        while (block.byteLength <= p.byteLength - offset) {
          totBytesOut += block.byteLength;
          p.set(block, offset);
          offset += block.byteLength;
          chunk = chunks.shift();
          if (!chunk) break;
          block = chunk.chunk;
        }
        if (offset > 0) {
          if (debug) console.log('pull', offset);
          return offset;
        }
      }
      // chunk too big, return as much as we can
      p.set(block.slice(0, p.byteLength), 0);
      block = block.slice(p.byteLength);
      chunks.unshift({
        chunk: block,
        hash: new Hash('md5').digest(block).hex()
      });
      totBytesOut += p.byteLength;
      if (debug) console.log('pull', p.byteLength);
      return p.byteLength;
    }

    if (totBytesIn != totBytesOut) {
      console.error('stream input/output mismatch');
    }
    return null;
  }

  async function handleBackPressure() {
    while (chunks.length >= 10) {
      await new Promise(resolve => setTimeout(resolve, 1));
    }
  };

  function writeSync(chunk: Uint8Array, isLast: boolean) {
    const hash = new Hash('md5').digest(chunk).hex();
    chunks.push({ chunk, hash });
    if (debug) status('writeSync ' + chunk.byteLength);
    totBytesIn += chunk.byteLength;
    eof = isLast;
  }

  async function write(chunk: Uint8Array, isLast: boolean) {
    if (!isLast) await handleBackPressure();
    writeSync(chunk, isLast);
  }

  function status(from: string = 'status') {
    if (debug) console.log(from, chunks.length, totBytesIn, totBytesOut, totBlockedBytesOut);
  }

  const reader: Deno.Reader = { read };
  return {
    handleBackPressure,
    writeSync,
    write,
    read,
    readChunked,
    reader,
    status,
    debug: (on: boolean) => debug = on,
  };
}

export {
  getUint8Pipe
};
