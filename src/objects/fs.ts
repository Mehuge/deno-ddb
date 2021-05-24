
import * as fs from 'https://deno.land/std@0.91.0/fs/mod.ts';
import * as path from 'https://deno.land/std@0.91.0/path/mod.ts';
import { hashFile, HashFile } from './_fs/hash-file.ts';
import { createHash } from 'https://deno.land/std@0.91.0/hash/mod.ts';
import { getUint8Pipe } from './u8pipe.ts';
import { FsFile } from './_fs/fs-file.ts';

function exists(fn: string | URL) {
  return fs.exists(path.resolve(fn.toString()));
}

function stat(fn: string | URL) {
  return Deno.lstat(fn);
}

function mkdir(fn: string | URL, mode?: number) {
  Deno.mkdir(fn, { mode });
}

function rmdir(fn: string | URL) {
  Deno.remove(fn);
}

async function mkdirp(fn: String, mode?: number) {
  const parts = fn.split(path.SEP_PATTERN);
  while (parts.length) {
    try {
      const stat = await Deno.lstat(path.join(...parts));
      if (stat.isDirectory) break;
      throw new Error(`mkdirp: part of path is a file: ${fn}`);
    } catch(e) {
      if (!(e instanceof Deno.errors.NotFound)) {
        throw e;
      }
    }
    parts.pop();
  }
  const start = parts.length;
  const add = fn.split(path.SEP_PATTERN);
  for (let i = start; i < add.length; i++) {
    parts.push(add[i]);
    await Deno.mkdir(path.join(...parts), { mode });
  }
}

/** open(file, flags)
 * file can be a string or URL.
 * flags can be one or a combination of
 *  r read, w write, c create, t truncate, a append
 */

function open(fn: string | URL, flags: string, mode: number) {
  const opts: Deno.OpenOptions = { mode };
  const r = flags.includes('r') || flags.includes('+');          // rw
  const w = flags.includes('w') || flags.includes('+');          // rw
  const c = flags.includes('c') || flags.includes('w');         // 'w' implies 'c'
  const a = flags.includes('a');
  const t = flags.includes('t') || flags.includes('w');         // 'w' implies truncate
  opts.read = r;
  opts.write = w || a || c || t;
  opts.append = a;
  opts.create = c;
  opts.truncate = t;
  return FsFile.open(fn, opts);
}

function writeFile(fn: string | URL, data: string, options?: Deno.WriteFileOptions) {
  return Deno.writeTextFile(fn, data, options);
}

function readFile(fn: string | URL) {
  return Deno.readTextFile(fn);
}

async function readdir(path: string | URL) {
  const entries = [];
  for await (const entry of Deno.readDir(path)) {
    entries.push(entry);
  }
  return entries;
}

function access(path: string | URL) {
  throw new Error("Not Implemented");
}

async function copy(from: string | URL | Deno.Reader, to: string | URL | Deno.Writer) {
  const fromFile = await FsFile.open(from, { read: true });
  const toFile = await FsFile.open(to, { write: true, create: true, truncate: true });
  await Deno.copy(fromFile.reader(), toFile.writer());
  if (toFile.isFile() && fromFile.isFile()) {
    const stat = await Deno.lstat(<string | URL>from);
    await chstat(<string | URL>to, stat);
  }
  fromFile.close();
  toFile.close();
}

function move(from: string, to: string) {
  return Deno.rename(from, to);
}

function link(from: string, to: string) {
  return Deno.link(from, to);
}

function unlink(fn: string | URL) {
  return Deno.remove(fn);
}

function chmod(fn: string | URL, mode: number) {
  return Deno.chmod(fn, mode);
}

function chown(fn: string, uid: number, gid: number) {
  return Deno.chown(fn, uid, gid);
}

async function chstat(fn: string | URL, stat: Deno.FileInfo, old?: Deno.FileInfo) {
  if (stat.mode !== null) {
    if (!old || (stat.mode != old.mode)) {
      await Deno.chmod(fn, stat.mode);
    }
  }
  if (stat.uid !== null && stat.gid !== null) {
    if (!old || (stat.uid !== old.uid || stat.gid !== old.gid)) {
      await Deno.chown(fn, stat.uid, stat.gid);
    }
  }
  if (stat.mtime && stat.atime) {
    if (!old || stat.mtime != old.mtime || stat.atime != old.atime) {
      await Deno.utime(fn instanceof URL ? fn.pathname : fn, stat.atime, stat.mtime);
    }
  }
}

function hash(file: string, options?: HashFile.Options) {
  return hashFile(file, options);
}

async function compare(a: string | Deno.File | Deno.Reader, b: string | Deno.File | Deno.Reader) {
  const h1 = await FsFile.open(a, { read: true });
  let h2: FsFile;
  try {
    h2 = await FsFile.open(b, { read: true });
  } catch(e) {
    h1.close();
    throw e;
  }
  const r1 = h1.reader();
  const r2 = h2.reader();
  const b1 = new Uint8Array(8192);
  const b2 = new Uint8Array(8192);
  const teardown = () => {
    h1.close();
    h2.close();
  }
  while (true) {
    const c1 = await r1.read(b1);
    const c2 = await r2.read(b2);
    if (c1 != c2) return teardown(), false;
    if (!c1) break;
    for (let i = 0; i < c1; i++) {
      if (b1[i] != b2[i]) return teardown(), false;
    }
  }
  teardown();
  return true;
}

// @deno-types="https://cdn.skypack.dev/fflate@0.6.8/lib/index.d.ts"
import * as fflate from 'https://cdn.skypack.dev/fflate@0.6.8?min';

/** pushToZipStream
 * reader: Source Stream
 * stream: Zipper/Unzipper
*/
interface ZipPipe {
  on: (what: string, handler: () => void) => void,
  cancel: () => void;
}

interface ZipPipeOptions {
  controller?: ReadableStreamDefaultController;
  bufSize?: number;
}

function pushToZipStream(reader: Deno.Reader, stream: any, opts: ZipPipeOptions = { bufSize: 65536 }): ZipPipe {
  let i = 0;
  let stop = false;
  let onstart: any;
  let onfinish: any;
  (async function() {
    for await (const block of Deno.iter(reader, { bufSize: opts.bufSize })) {
      if (stop) return;
      if (opts.controller) {
        const desiredSize = opts.controller.desiredSize || 1;
        if (desiredSize < 1) {
          await new Promise($ => setTimeout($,1));
        }
      }
      const hash = createHash('md5');
      hash.update(block);
      stream.push(block);
      if (i == 0 && onstart) onstart();
      i++;
    }
    stream.push(new Uint8Array(0), true);
    if (onfinish) onfinish();
  })();
  return {
    on: function (what: string, handler: () => any | null) {
      switch(what) {
        case 'start': onstart = handler; break;
        case 'finish': onfinish = handler; break;
      }
    },
    cancel() {
      stop = true;
    }
  }
}

function zip(from: string | URL | Deno.File, to: string | URL | Deno.File, options = {}) {
  return new Promise<void>(async (resolve, reject) => {
    const hFrom = await FsFile.open(from, { read: true });
    const hTo = await FsFile.open(to, { write: true, create: true, truncate: true });
    const zipper: any = new fflate.Gzip({ level: 9 }, (chunk: Uint8Array, isLast: boolean) => {
      (<Deno.File>hTo.stream).writeSync(chunk);
      if (isLast) {
        hTo.close();
        resolve();
      }
    });
    pushToZipStream(hFrom.reader(), zipper).on('finish', () => hFrom.close());
  });
}

function unzip(from: string | URL | Deno.Reader, to: string | URL | Deno.File | null = null) {
  return new Promise<void>(async (resolve, reject) => {
    const hFrom = await FsFile.open(from, { read: true });
    const hTo = to ? await FsFile.open(to, { write: true, create: true, truncate: true }) : null;
    const unzipper: any = new fflate.Gunzip();
    if (hTo) {
      // To file/stream, write unzipped content to file/stream
      unzipper.ondata = (chunk: Uint8Array, isLast: boolean) => {
        (<Deno.File>hTo.stream).writeSync(chunk);
        if (isLast) {
          hTo.close();
          resolve();
        }
      };
    }
    const pipe = pushToZipStream(hFrom.reader(), unzipper);
    pipe.on('start', () => {
      // No to file/stream given, we return the unzipper, after we send
      // the first block.
      if (!hTo) resolve(unzipper);
    });
    pipe.on('finish', () => hFrom.close());
  });
}

function compareZipWith(compressed: string | URL | Deno.File, file: string  | URL | Deno.File ) {
  let same = true;
  return new Promise<boolean>(async (resolve, reject) => {
    const text = new TextDecoder();
    const not_same = () => { resolve(same = false); };
    const hZipped = await FsFile.open(compressed, { read: true });
    const hFile = await FsFile.open(file, { read: true });
    const unzipper: any = new fflate.Gunzip((chunk: Uint8Array, isLast: boolean) => {
      let buffer = new Uint8Array(chunk.byteLength);
      let size;
      if (chunk.byteLength > 0) {
        size = (<Deno.File>hFile.stream).readSync(buffer);
        if (size == null) return not_same();
        if (size != chunk.length) return not_same();
        for (let i = 0; i < size; i++) {
          if (buffer[i] != chunk[i]) return not_same();
        }
      }
      if (isLast) {
        buffer = new Uint8Array(1);
        size = (<Deno.File>hFile.stream).readSync(buffer);
        if (size != null) return not_same();
        resolve(true);
      }
    });
    for await (const block of Deno.iter(hZipped.reader())) {
      if (!same) break;
      unzipper.push(block);
    }
    if (same) unzipper.push(new Uint8Array(0), true);
    hFile.close();
    hZipped.close();
  });
}

import { readLines } from "https://deno.land/std@0.90.0/io/bufio.ts";

interface Handlers {
  line?: (line: string) => void;
  error?: (error: string) => void;
  close?: () => void;
}

function readline(file: string | URL | Deno.File) {
  const handlers: Handlers = {};
  (async function() {
    try {
      const hFile = await FsFile.open(file, { read: true });
      for await (const line of readLines(hFile.reader())) {
        if (handlers.line) handlers.line(line);
      }
      hFile.close();
      if (handlers.close) handlers.close();
    } catch(e) {
      if (handlers.error) handlers.error(e);
      else throw e;
    }
  })();
  return {
    on: function(when: string, handler: any) {
      switch(when) {
        case 'line': handlers.line = handler; break;
        case 'error': handlers.error = handler; break;
        case 'close': handlers.close = handler; break;
      }
    }
  }
}

function zipToReadStream(from: string | URL | Deno.File) {
  let pipe: ZipPipe;
  const queueingStrategy = { highWaterMark: 10 };
  return new ReadableStream({
    start: async (controller: ReadableStreamDefaultController) => {
      const hFrom = await FsFile.open(from, { read: true });
      const zipper = new fflate.Gzip({ level: 9 }, (chunk: Uint8Array, isLast: boolean) => {
        controller.enqueue(chunk);
        if (isLast) {
          hFrom.close();
          controller.close();
        }
      });
      pipe = pushToZipStream(hFrom.reader(), zipper, { bufSize: 65536, controller });
    },
    type: "bytes",
    cancel: () => pipe.cancel(),
  }, queueingStrategy);
}

function zip2stream(from: string | URL | Deno.File) {
  const upipe = getUint8Pipe();
  return new Promise<Deno.Reader>(async (resolve, reject) => {
    let blocks = 0;
    const hFrom = await FsFile.open(from, { read: true });
    const zipper = new fflate.Gzip({ level: 9 }, (chunk: Uint8Array, isLast: boolean) => {
      upipe.writeSync(chunk, isLast);
      if (++blocks == 1) resolve(upipe.reader);
    });
    for await (const block of Deno.iter(hFrom.reader())) {
      await upipe.handleBackPressure();
      zipper.push(block);
    }
    zipper.push(new Uint8Array(0), true);
    hFrom.close();
  });
}

async function hashFileZip(zip: string, options: HashFile.Options = { blockSize: 16384 }) {
  const hZip = await Deno.open(zip, { read: true });
  let chunks: Uint8Array[] = [];
  const unzipper = new fflate.Gunzip((chunk: Uint8Array, isLast: boolean) => {
    if (chunk.byteLength > 0 || isLast) chunks.push(chunk);
  });
  let eof = false;
  const reader: Deno.Reader = {
    async read(block: Uint8Array) {
      while (!chunks.length) {
        if (eof) return null;
        // no chunks queued, read the next block from the zip file, and unzip it, which will
        // trigger an ondata call.
        const raw = new Uint8Array(<number>options.blockSize);
        const read = await hZip.read(raw);
        if (read == null) {
          unzipper.push(new Uint8Array(0), true);
          eof = true;
          hZip.close();
        } else {
          unzipper.push(raw.subarray(0, read));
        }
      }
      const uncompressed = <Uint8Array>chunks.shift();
      if (uncompressed.byteLength == 0 && eof) return null;
      if (uncompressed.byteLength > block.byteLength) {
        // block too big to fit in return buffer, sed as much as we can
        // and save the rest until later.
        block.set(uncompressed.subarray(0, block.byteLength));
        chunks.unshift(uncompressed.subarray(block.byteLength));
        return block.byteLength;
      }
      // return uncompressed block
      block.set(uncompressed, 0);
      return uncompressed.byteLength;
    }
  };
  return await hashFile(reader, options);
}

async function hashZip(zip: string | URL, options: HashFile.Options = { encoding: 'hex', blockSize: 65536 }) {
  const hash = createHash('sha256');
  const hZip = await Deno.open(zip, { read: true });
  const unzipper = new fflate.Gunzip((chunk: Uint8Array, isLast: boolean) => {
    hash.update(chunk);
  });
  for await (const block of Deno.iter(hZip, { bufSize: options.blockSize })) {
    unzipper.push(block);
  }
  unzipper.push(new Uint8Array(0), true);
  hZip.close();
  return hash.toString(options.encoding);
}

interface Signature {
  blockSize: number;
  blocks: string[];
  hash?: string;
}

async function signature(source: string, opts: HashFile.Options): Promise<Signature> {
  const signature: Signature = { blockSize: opts.blockSize || 16384, blocks: [] };
  opts = Object.assign({}, opts);
  opts.signature = ({ checksum } : HashFile.SignaturePart) => signature.blocks.push(checksum);
  signature.hash = await hashFile(source, opts);
  return signature;
}

export {
  FsFile,
  stat,
  mkdir,
  rmdir,
  mkdirp,
  open,
  writeFile,
  readFile,
  readdir,
  exists,
  copy,
  move,
  link,
  unlink,
  chmod,
  chown,
  chstat,
  compare,
  zip,
  unzip,
  compareZipWith,
  readline,
  zip2stream,
  zipToReadStream,
  hash,
  hashZip,
  hashFile,
  hashFileZip,
  signature,
  getUint8Pipe,
}
