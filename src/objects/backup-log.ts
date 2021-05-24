
import { FsFile } from './fs.ts';
import * as fs from './fs.ts';
import * as path from 'https://deno.land/std@0.91.0/path/mod.ts';

const VERSION = 2;

export namespace BackupLog {
  export interface ConstructorParameters {
    root: string;
    userid: string;
    setname: string;
  }
  export interface FileEntry {
    type: 'D' | 'F';
    mode: number | string;
    uid: number | string;
    gid: number | string;
    ctime: Date | string;
    mtime: Date | string;
    size: number;
    hash: string;
    path: string;
  }
  export interface HashMapEntry {
    count: number;
  }
  export interface HashMap {
    [key: string]: HashMapEntry;
  }
  export interface Stats {
    total: number;
  }
  export interface Header {
    type: 'HEADER';
    version: string;
  }
  export interface Source {
    type: 'SOURCE';
    root: string;
  }
  export interface Status {
    type: 'STATUS';
    status: string;
    stats: Stats,
  }
  export interface Unknown {
    type: 'UNKNOWN';
    line: string;
  }
  export interface InfoFileEntry {
    source: string;
    ctime: Date | string;
    mtime: Date | string;
    size: number | '-';
    hash: string;
  }
  export interface BackupInfo {
    time: Date;
    F: { [path: string]: InfoFileEntry };
    D: { [path: string]: InfoFileEntry };
  }
}

export type ConstructorParameters = BackupLog.ConstructorParameters;
export type HashMap = BackupLog.HashMap;
export type BackupLogFileEntry = BackupLog.FileEntry;
export type BackupLogEntry = BackupLog.Header | BackupLog.FileEntry | BackupLog.Status | BackupLog.Source | BackupLog.Unknown;
export type InfoFileEntry = BackupLog.InfoFileEntry;
export type BackupInfo = BackupLog.BackupInfo;

export class BackupLog {

  private root: string;
  private userid: string;
  private setname: string;

  private log?: FsFile;

  constructor({ root, userid, setname }: ConstructorParameters) {
    this.root = root;
    this.userid = userid||'';
    this.setname = setname;
  }

  _log(): FsFile {
    if (!this.log) throw new Error("Log file is not open");
    return this.log;
  }

  static parseWhen(when: string | Date = 'current') {
    switch(when) { case 'current': case 'running': return when; }
    const isDate = Object.prototype.toString.call(when) == '[object Date]';
    return (isDate ? (<Date>when).toISOString() : <string>when).replace(/[\-:\.]/g,'');
  }

  static ext2iso(ext: string) {
    return `${ext.substr(0,4)}-${ext.substr(4,2)}-${ext.substr(6,2)}`
          + `T${ext.substr(9,2)}:${ext.substr(11,2)}:${ext.substr(13,2)}`
          + `.${ext.substr(15)}`;
  }

  getLogName(when: string) {
    return path.join(this.root, 'backups', this.userid, `${this.setname}.${when}`);
  }

  async exists(when: string) {
    try {
      await fs.exists(this.getLogName(when));
      return true;
    } catch(e) {
      // does not exist, return undefined
    }
  }

  async create(when: string) {
    const dir = path.join(this.root, 'backups', this.userid);
    await fs.mkdirp(dir, 0o755);
    this.log = await fs.open(this.getLogName(when), 'w', 0o600);
    await this._log().appendFile(`V${VERSION} type uid:gid:mode ctime mtime - size hash 0 path\n`);
  }

  async writeSourceEntry({ root } : { root: string }) {
    await this._log().appendFile(`SOURCE ${root}\n`);
  }

  static entryToString({ type, mode, uid, gid, ctime, mtime, size, hash, path }: BackupLogFileEntry) {
    if (typeof mode != 'string') mode = mode.toString(8);
    if (typeof ctime != 'string') ctime = ctime.toISOString();
    if (typeof mtime != 'string') mtime = mtime.toISOString();
    if (typeof size != 'string') size = size|0;
    return `${type} ${uid}:${gid}:${mode} ${ctime} ${mtime} - ${size} ${hash} 0 ${JSON.stringify(path.replace(/\\/g,'/'))}`;
  }

  async writeEntry(entry: BackupLogFileEntry) {
    await this._log().appendFile(`${BackupLog.entryToString(entry)}\n`);
  }

  async finish(status = 'OK') {
    await this._log().appendFile(`V${VERSION} STATUS ${status}\n`);
    this._log().close();
  }

  getLinesFromLog(when: string | Date) {
    const lines: BackupLogEntry[] = [];
    return new Promise<BackupLogEntry[]>((resolve, reject) => {
      const readline = fs.readline(this.getLogName(BackupLog.parseWhen(when)));
      readline.on('line', (line: string) => {
        if (line.length > 0) {
          const parsed = BackupLog.parse(line);
          if (parsed) lines.push(parsed);
        }
      });
      readline.on('error', reject);
      readline.on('close', () => resolve(lines));
    });
  }

  getHashesFromInstanceLog(when: string | Date, hashes: HashMap) {
    return new Promise<void>((resolve, reject) => {
      const readline = fs.readline(this.getLogName(BackupLog.parseWhen(when)));
      readline.on('line', (line: string) => {
        const entry = BackupLog.parse(line);
        if (entry.type == 'F') {
          const key = `${entry.hash}.${entry.size}`;
          const hash = hashes[key] || { count: 0 };
          hash.count ++;
          hashes[key] = hash;
        }
      });
      readline.on('close', resolve);
    });
  }

  async getLastBackup() {
    const dir = path.join(this.root, 'backups', this.userid || '');
    const prefix = `${this.setname}.`;
    let lastBackup;
    try {
      for (const log of await fs.readdir(dir)) {
        if (log.isFile && log.name[0] != '.') {
          if (log.name.startsWith(prefix)) {
            const ts = log.name.substr(prefix.length);
            switch(ts) {
              case 'running': case 'current':
                break;
              default:
                if (!lastBackup || ts > lastBackup) lastBackup = ts;
            }
          }
        }
      }
    } catch(e) {
      console.log(e);
    }
    if (lastBackup) {
      const log = await this.getLinesFromLog(lastBackup);
      const F: { [path: string]: InfoFileEntry } = {};
      const D: { [path: string]: InfoFileEntry } = {};
      const info: BackupInfo = {
        time: new Date(BackupLog.ext2iso(lastBackup)),
        F,
        D,
      };
      let source: string;
      log.forEach(entry => {
        switch(entry.type) {
          case 'SOURCE':
            source = path.join(entry.root);
            break;
          case 'F': case 'D':
            const { hash, size } = entry;
            info[entry.type][path.join(source, entry.path)] = {
              hash, size, source,
              mtime: new Date(<string>entry.mtime),
              ctime: new Date(<string>entry.ctime),
            };
            break;
        }
      });
      return info;
    }
  }

  static parse(line: string): BackupLogEntry {
    const words = line.split(' ');
    switch(words[0]) {
    case 'V1': case 'V2':
      switch(words[1]) {
        case 'STATUS':
          return { type: 'STATUS', status: words[2], stats: JSON.parse(words.slice(3).join(' ')) };
      }
      return { type: 'HEADER', version: words[0][1] };
    case 'SOURCE':
      return { type: 'SOURCE', root: line.substr(words[0].length+1) };
    case 'D': case 'F':
      const path = JSON.parse(words.slice(8).join(' '));
      const mode = words[1].split(':');
      if (mode.length == 1) {
        mode.unshift('');
        mode.unshift('');
      }
      return {
        type: words[0],
        uid: mode[0] && (<any>mode[0])|0,
        gid: mode[1] && (<any>mode[1])|0,
        mode: mode[2],
        ctime: words[2],
        mtime: words[3],
        size: words[0] == 'D' ? 0 : (<any>words[5])|0,
        hash: words[6],
        /* ignore words[7] - obsolete variant */
        path,
      };
    }
    return { type: 'UNKNOWN', line: line };
  }

  async complete(ts = new Date()) {
    const from = this.getLogName('running');
    const to = this.getLogName(BackupLog.parseWhen(ts));
    const current = this.getLogName('current');
    await fs.move(from, to)
    if (await fs.exists(current)) await fs.unlink(current);
    debugger;
    try {
      await fs.link(to, current);
    } catch(e) {
      if (e.message.includes('(os error 1)')) {
        // get this on FAT32 filesystem where links are not supported
        // so copy instead.
        await fs.copy(to, current);
      } else {
        throw e;
      }
    }
  }
}

export default BackupLog;
