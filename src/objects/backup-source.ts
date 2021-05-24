
import * as fs from './fs.ts';
import * as path from 'https://deno.land/std@0.91.0/path/mod.ts';
import Filter from './filter.ts';
import { BackupInfo, InfoFileEntry } from './backup-log.ts';
import { BackupStats } from './backup-set.ts';
import { BackupInstance } from './backup-instance.ts';
import { memtrack } from './debug.ts';

function sameAsLastBackup(a: Deno.FileInfo, b: InfoFileEntry) {
  if (typeof b.mtime == 'string') throw new Error('sameAsLastBackup expects mtime to be a date');
  if (a.size != b.size) return false;
  if (a.mtime && b.mtime) {
    if (a.mtime.getTime() != (<Date>b.mtime).getTime()) return false;     // TODO: Can this be a string?
  } else {
    if (a.mtime != b.mtime) return false;
  }
  return true;
}

export class BackupSource {
  private src: string = '.';
  private filters: string[] | undefined;
  private subdirs: string[] | undefined;
  private ignore: string[] | undefined;       // TODO: What's this for?
  private deepscan: boolean = false;
  private verbose: boolean = false;
  private checkHash: boolean = false;
  private lastBackup: BackupInfo | null = null;
  constructor({ src, filters, subdirs, deepscan, verbose, checkHash }: { src: string, filters: string[], subdirs: string[], deepscan: boolean, verbose: boolean, checkHash: boolean }) {
    this.src = src;
    if (!src) throw new Error("source path missing");
    this.filters = filters;
    this.subdirs = subdirs;
    this.deepscan = deepscan;
    this.verbose = verbose;
    this.checkHash = checkHash;
  }

  async backupTo(instance: BackupInstance, stats: BackupStats, lastBackup: BackupInfo) {
    this.lastBackup = lastBackup;
    instance.log().writeSourceEntry({ root: this.src });
    const filter = new Filter({ filters: this.filters });
    const subdirs = this.subdirs;
    if (subdirs && subdirs.length) {
      for (const subdir of subdirs) {
        await this._backupDir(path.join(this.src, subdir), instance, stats, filter);
      }
    } else {
      await this._backupDir(this.src, instance, stats, filter);
    }
  }

  async _log(instance: BackupInstance, type: 'F' | 'D', fn: string, stats: Deno.FileInfo, hash = '-', modified = ' ') {
    if (this.verbose) console.log(modified, path.join(this.src, fn));
    const { mode, birthtime, mtime, size } = stats;
    let { uid, gid } = stats;
    if (Deno.build.os === "windows") {
      gid = uid = null;
    };
    await instance.log().writeEntry({
      type,
      uid: uid || '',
      gid: gid || '',
      mode: mode || '',
      ctime: birthtime || '',
      mtime: mtime || '',
      size,
      hash,
      path: fn
    });
  }

  async _scanDir(dirname: string, instance: BackupInstance, stats: BackupStats, filter: Filter) {
    // if (this.verbose) console.log(`scan ${dirname}`);
    try {
      const dir = await fs.readdir(dirname);
      const l = this.src.length+1;
      for (let i = 0; i < dir.length; i++) {
        const entry = dir[i];
        if (entry.isDirectory) {
          const fn = path.join(dirname, entry.name);
          const ignored = filter.ignores(fn.substr(l));
          if (ignored) {
            await this._scanDir(fn, instance, stats, filter);
          } else {
            await this._backupDir(fn, instance, stats, filter);
          }
        }
      }
    } catch(e) {
      console.error(e);
    }
  }

  async _backupDir(dirname: string, instance: BackupInstance, stats: BackupStats, filter: Filter) {
    try {
      const fstat = await fs.stat(dirname);
      if (fstat) {
        let modified = 'a';
        if (this.lastBackup) {
          const last = this.lastBackup.D[dirname];
          if (last && fstat.mtime && fstat.mtime > this.lastBackup.time) {
            modified = 'u';
          } else {
            modified = '-';
          }
        }
        memtrack();
        await this._log(instance, 'D', dirname.substr(this.src.length+1), fstat, '-', modified);
      }
    } catch(e) {
      debugger;
      if (e.code == 'ENOENT') {   // TODO: This won't be correct
        console.log(`${dirname} is missing`);
        stats.skipped++;
        return;
      }
      throw e;
    }
    stats.folders ++;
    try {
      const dir = await fs.readdir(dirname);
      const l = this.src.length+1;
      const deepscan = this.deepscan;
      for (let i = 0; i < dir.length; i++) {
        const entry = dir[i];
        let type;
        if (entry.isFile) type = 'F';
        if (entry.isDirectory) type = 'D';
        if (entry.isSymlink) type = 'L';
        if (type) {
          const fn = path.join(dirname, entry.name);
          const ignored = filter.ignores(fn.substr(l));
          switch(type) {
            case 'D':
              if (ignored) {
                if (deepscan) {
                await this._scanDir(fn, instance, stats, filter)
                }
              } else {
                await this._backupDir(fn, instance, stats, filter);
              }
              break;
            case 'F':
              if (!ignored) await this._backupFile(fn, instance, stats);
              break;
            default:
              // ignore other types
              break;
          }
        } else {
          console.log(`${entry.name} unknown file type`);
        }
      }
    } catch(e) {
      console.error(e);
    }
  }

  async _backupFile(fn: string, instance: BackupInstance, stats: BackupStats) {
    try {
      const fstat = await fs.stat(fn);
      if (fstat) {
        let hash;
        let modified = 'a';
        if (this.lastBackup) {
          const last = this.lastBackup.F[fn];
          if (last && fstat.mtime) {
            if (fstat.mtime <= this.lastBackup.time && sameAsLastBackup(fstat, last)) {
              if (this.checkHash) {
                modified = 'c';
              } else {
                hash = last.hash;
                modified = '-';
              }
            } else {
              modified = 'u';
            }
          }
        }
        if (!hash) {
          hash = await fs.hash(fn, { encoding: 'hex' });
        }
        stats.files ++;
        stats.bytes += fstat.size;
        const { variant, stored } = await instance.put(fn, fstat.size, hash);
        memtrack();
        await this._log(instance, 'F', fn.substr(this.src.length+1), fstat, hash, modified);
        if (stored) {
          stats.backedUp.files ++;
          stats.backedUp.bytes += fstat.size;
        }
      }
    } catch(e) {
      stats.skipped ++;
      console.log(`${fn} failed`);
      console.dir(e);
      throw e;
    }
  }
};

export default BackupSource;
