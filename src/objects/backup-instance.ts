import BackupLog, { HashMap } from './backup-log.ts';
import BackupFileSystem from './backup-filesystem.ts';
import BackupTarget from './backup-target.ts';
import BackupSource from './backup-source.ts';
import * as path from 'https://deno.land/std@0.91.0/path/mod.ts';
import * as fs from './fs.ts';
import Filter from './filter.ts';
import { memtrack } from './debug.ts';

/*
 * A backup instance represents a single run of a backup and is associated with the instance
 * log file in ./backups/ in the backup target.
 */

export class BackupInstance {
  private target: BackupTarget | undefined;
  private setname: string | undefined;
  private userid: string | undefined;
  private _log: BackupLog | undefined;
  constructor({ target, setname, userid }: { target: BackupTarget, setname: string, userid: string }) {
    this.target = target;
    this.setname = setname;   // the backup set name
    this.userid = userid;
  }

  _userid() {
    if (!this.userid) throw new Error('userid not defined');
    return this.userid;
  }

  _setname() {
    if (!this.setname) throw new Error('setname not defined');
    return this.setname;
  }

  async createNewInstance() {
    this._log = new BackupLog({ root: this.target.getPath(), userid: this._userid(), setname: this._setname() });
    await this._log.create('running');
  }

  async complete(ts: Date) {
    await this.log().complete(ts);
  }

  log() {
    if (!this._log) {
      this._log = new BackupLog({ root: this.target.getPath(), userid: this._userid(), setname: this._setname() });
    }
    return this._log;
  }

  async exists(when: string) {
    return await this.log().exists(when);
  }

  async verify({ when, log, verbose, compare, compareWith }: { when: string, log: (s: string) => void, verbose: boolean, compare: boolean, compareWith?: string }) {
    const bfs = this.target.fs();
    const lines = await this.log().getLinesFromLog(when);
    function LOG(s: string) {
      (log || console.log)(s);
    }
    let root;
    for (let i = 0; i < lines.length; i++) {
      const entry = lines[i];
      switch (entry.type) {
      case 'SOURCE':
        if (verbose) LOG(`SOURCE ${entry.root}`);
        root = entry.root;
        break;
      case 'F':
        if (!root) throw new Error('corrupt backup log, missing SOURCE entry');
        try {
          const compareWithFile = compare ? path.join(compareWith || root, entry.path) : null;
          await bfs.verify(entry.size, entry.hash, compareWithFile);
          memtrack();
          if (verbose) LOG(`OK ${entry.hash} ${entry.size} ${entry.path}`);
        } catch(e) {
          if (e.code == 'ENOCOMPARE') {
            LOG(`CHANGED ${entry.hash} ${entry.size} ${entry.path}`);
          } else if (e.code == 'ENOENT') {
            LOG(`DELETED ${entry.hash} ${entry.size} ${entry.path}`);
          } else {
            LOG(`ERROR ${entry.hash} ${entry.size} ${entry.path}`);
            LOG(e);
          }
        }
        break;
      }
    }
  }

  async getLinesFromInstanceLog(when: string) {
    return await this.log().getLinesFromLog(when);
  }

  async getHashesFromInstanceLog(when: string, hashes: HashMap) {
    return await this.log().getHashesFromInstanceLog(when, hashes);
  }

  async restore({ when, filter, sources, output, verbose = false }: { when: string, filter: Filter, sources: BackupSource[], output: string, verbose: boolean }, remoteRestore: any) {      // TODO what is remoteResore
    // restore requires that we:-
    // specify a backup set
    // specify an instance (support for woolyer slection to come later --after <date> sort of thing)
    // specify a source path (or not, to mean all)
    const lines = await this.log().getLinesFromLog(when);
    const search = new Filter({ filterItems: filter.getFilters() });
    const from = this.target.fs();
    let root;
    for (let i = 0; i < lines.length; i++) {
      const entry = lines[i];
      switch(entry.type) {
        case 'HEADER': case 'STATUS': case 'UNKNOWN':
          break;
        case 'SOURCE':
          root = entry.root;
          if (remoteRestore) await remoteRestore({ type: 'SOURCE', root });
          if (sources) {
            console.log('TODO: what are we supposed to do here?');
            console.dir(sources);
            // find this source in sources, and create filter if found
            // TODO:
            // sourceFilter = new Filter({ excludes: this.exclude, includes: this.include });
          }
          break;
        case 'D':
        case 'F':
          if (!root) throw new Error('missing root (corrupt instance?) use --output to override');
          if (!search.ignores(entry.path)) {
            if (remoteRestore) {
              await remoteRestore(entry);
            } else {
              let doRestore = true;
              const file = path.join((output || root), entry.path);
              switch(entry.type) {
              case 'F':
                try {
                  const stat = await fs.stat(file);   // will fail with ENOENT if missing
                  const localHash = await this.target.fs().hashFile(file);
                  if (localHash == entry.hash && stat.size == entry.size) {
                    const old: Deno.FileInfo = Object.assign({}, stat, {
                      size: entry.size,
                      mode: typeof entry.mode == 'string' ? parseInt(entry.mode, 8) : entry.mode,
                      mtime: new Date(entry.mtime),
                      atime: new Date()
                    });
                    await fs.chstat(file, stat, old);
                    console.log(`${file} not changed`);
                    doRestore = false;
                  }
                } catch(e) {
                  if (e.code == 'EPERM') {
                    console.error(e.message);
                    doRestore = false;
                  } else {
                    if (e.code != 'ENOENT') throw e;
                  }
                }
                break;
              }
              doRestore && await BackupInstance.restoreEntry({ from, entry, to: file });
            }
          }
          break;
      }
    }
  }

  static async restoreEntry({ from, entry, to, verbose = false, isCompressedStream = false }: { from: BackupFileSystem | Deno.Reader, entry: BackupLog.FileEntry, to: string, verbose: boolean, isCompressedStream: boolean }) {
    memtrack();
    switch(entry.type) {
      case 'D':
        // re-create directory
        if (verbose) console.log(to);
        await fs.mkdirp(to, parseInt(<string>entry.mode,8)|0o100);   // give at least excute to creator on directories
        break;
      case 'F':
        // re-create file
        if (verbose) console.log(to);
        if (from instanceof BackupFileSystem) {
          await from.restore(entry.size, entry.hash, to);
        } else {
          // restore from stream or file (latter not actually used anywhere)
          if (isCompressedStream) {
            await fs.unzip(from, to);
          } else {
            await fs.copy(from, to);
          }
        }
      try {
        const stat = {
          mode: typeof entry.mode == 'string' ? parseInt(entry.mode,8) : entry.mode,
          mtime: new Date(entry.mtime),
          birthtime: new Date(entry.ctime),
          atime: new Date(),
        };
        await fs.chstat(to, <Deno.FileInfo>stat);
      } catch(e) {
        if (e.code != 'EPERM') throw e;
        console.error(e.message);
      }
      break;
    }
  }

  async put(file: string, size: number, hash: string) {
    const bfs = this.target.fs();
    return await bfs.put(file, size, hash);
  }
}

export default BackupInstance;
