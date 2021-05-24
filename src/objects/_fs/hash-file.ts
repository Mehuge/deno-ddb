
import { createHash } from "https://deno.land/std@0.90.0/hash/mod.ts";
import * as path from 'https://deno.land/std/path/mod.ts';

export namespace HashFile {
  export interface SignaturePart {
    offset: number,
    size: number,
    checksum: string,
  }
  export interface Options {
    encoding?: 'base64' | 'hex';
    blockSize?: number;
    bufSize?: number;
    signature?: (args: SignaturePart) => void;
    blockHash?: string;
  }
}

export async function sha256(fn: string) {
  const sha256 = createHash('sha256');
  const file = await Deno.open(fn, { read: true });
  for await (const block of Deno.iter(file)) {
    sha256.update(block);
  }
  file.close();
  return sha256.toString('hex');
}

export async function hashFile(source: string | URL | Deno.File | Deno.Reader, opts: HashFile.Options = {}) {
  const encoding = opts.encoding || 'hex';
  const blockSize = opts.blockSize || 16384;
  const bufSize = opts.bufSize || 65536;
  const signature = opts.signature;
  const sum = createHash('sha256');
  let blockSum = createHash('sha1');
  let blocks = 0;
  let blockLen = 0;
  let offset = 0;
  let close = false;
  if (typeof source == "string" || source instanceof URL) {
    source = await Deno.open(source);
    close = true;
  }
  for await (let chunk of Deno.iter(source, { bufSize })) {
    sum.update(chunk);
    if (signature) {
      while (blockLen + chunk.length >= blockSize) {
        const slice = chunk.slice(0, blockSize - blockLen);
        blockSum.update(slice);
        signature({ offset, size: blockSize, checksum: blockSum.toString(encoding) });
        ++blocks;
        offset = blocks * blockSize;
        chunk = chunk.slice(blockSize - blockLen);
        blockSum = createHash('sha1');
        blockLen = 0;
      }
      if (chunk.length) {
        blockSum.update(chunk);
        blockLen += chunk.length;
      }
    }
  }
  if (signature && blockLen > 0) {
    signature({ offset, size: blockLen, checksum: blockSum.toString(encoding) });
  }
  if (close) (<Deno.File>source).close();
  return sum.toString(encoding);
}
