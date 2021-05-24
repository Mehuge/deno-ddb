import * as path from "https://deno.land/std/path/mod.ts";
import { exists } from 'https://deno.land/std/fs/exists.ts';
import * as fs from './fs.ts';

export interface Options {
  root: string;
}

export interface Key {
  hash: string;
  size: number;
  name: string;
  path: string;
}

export default class HashFileSystemV5 {
  root: string;

  constructor(opts: Options) {
    const { root } = opts;
    this.root = root;
  }

  _hash2name(hash: string) {
    return path.join(`${hash.substr(0,2)}`, `${hash.substr(2,2)}`, hash);
  }

  getKey(hash: string, size: number): Key {
    const name = this._hash2name(hash);
    return { hash, size, name, path: path.join(this.root, `${name}.${size}`) };
  }

  async exists(key: Key) {
    return await exists(key.path);
  }

  async store(file: string, key: Key, isCompressedStream: boolean) {
    debugger;
    await fs.mkdirp(path.join(this.root, path.dirname(key.name)), 0o700);
    if (isCompressedStream) {
      await fs.copy(file, key.path);
    } else {
      await fs.zip(file, key.path);
    }
  }

  async restore(key: Key, file: string, isCompressedStream: boolean) {
    if (isCompressedStream) {
      await fs.copy(key.path, file);
    } else {
      await fs.unzip(key.path, file);
    }
  }

  compare(key: Key, file: string) {
    return fs.compareZipWith(key.path, file);
  }

  hashKey(key: Key) {
    return fs.hashZip(key.path, { encoding: 'hex' });
  }

  static hashFile(file: string) {
    return fs.hashFile(file, { encoding: 'hex' });
  }

  hashFile(file: string) {
    return HashFileSystemV5.hashFile(file);
  }

  keyFromFile(dir: string, name: string) {
    return name;
  }

}
