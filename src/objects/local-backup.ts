
import * as fs from './fs.ts';
import * as path from 'https://deno.land/std@0.91.0/path/mod.ts';
import BackupInstance from './backup-instance.ts';
import BackupLog from './backup-log.ts';
import BackupSet from './backup-set.ts';
import BackupFileSystem from './backup-filesystem.ts';
import Filter from './filter.ts';
import { memtrack } from './debug.ts';

const VERSION = 1;

interface BackupConfig {
  version: number;
  fstype: 'hash-v5';
  saved?: Date;
}

interface Logs {
  userid?: string;
  log: Deno.DirEntry;
}

interface ListOptions {
  log?: (s: string) => void;
  filter?: string[];
  sources: BackupSource[];
  since?: Date;
  userid?: string;
  setname: string;
  when?: string;
};

interface VerifyOptions {
  setname: string;
  when: string;
  userid?: string;
  compare?: boolean;
  compareWith?: string;
  verbose?: boolean;
  log?: (s: string) => void;
}

interface Hashes {
  [hash: string]: {
    seen?: boolean;
  }
}

class LocalBackup {
  private destination: string;
  private filesystem: BackupFileSystem;
  private fstype: string;
  private configFile: string;
  private filesDb: string;
  private backups: string;
  private config: BackupConfig;
  private _log?: BackupLog;

  constructor({ destination, fstype }: { destination: string, fstype: string }) {
    if (!destination) throw new Error("Backup destination not supplied");
    this.destination = destination;
    this.filesystem = new BackupFileSystem();
    this.fstype = fstype;
    this.configFile = path.join(this.destination, 'config.json');
    this.filesDb = path.join(this.destination, 'files.db');
    this.backups = path.join(this.destination, 'backups');
    this.config = { version: VERSION, fstype: 'hash-v5' };
  }

  async initConfig() {
    await this.saveConfig();
  }

  async saveConfig() {
    this.config.saved = new Date();
    await fs.writeFile(this.configFile, JSON.stringify(this.config, null, '  '));
  }

  async loadConfig() {
    this.config = JSON.parse(await fs.readFile(this.configFile));
  }

  async connect(create: boolean) {

    // if writing to the destination need to make sure it exists
    if (create) {
      try {
        const stats = await fs.stat(this.destination);
        if (!stats.isDirectory) throw new Error('destination not a directory');
      } catch(e) {
        if (e.code == 'ENOENT') await fs.mkdirp(this.destination);
        else throw e;
      }

      // If directory is not initialised, initialise it
      await fs.mkdirp(this.filesDb);
      await fs.mkdirp(this.backups);

      // Create config if it doesn't exist
      if (!await fs.exists(this.configFile)) {
        this.initConfig();
      }
    }

    if (!this.config) {
      // load the config
      await this.loadConfig();
      if (!this.config) {
        throw new Error(`${this.destination} backup destination does not exist`);
      }
    }

    if (this.fstype && this.config.fstype != this.fstype) {
      throw new Error(
        `${this.destination} is fstype ${this.config.fstype} which does not match`
        + ` the requested fstype ${this.fstype}.\n`
        + `See --fs-type option.\n`
        + `Note: Backup server destinations must be hash-v5.`);
    }

    // tell the filesystem where it is
    await this.filesystem.setLocation(this.filesDb, this.config.fstype);
  }

  getConfig() {
    return this.config;
  }

  getPath() {
    return this.destination;
  }

  toString() {
    return "BackupDest: " + this.destination;
  }

  fs() {
    return this.filesystem;
  }

  async getLogs(setname?: string, when?: string, userid?: string, logs: Logs[] = []) {
    const dir = path.join(this.backups, userid || '');
    for (const log of await fs.readdir(dir)) {
      if (log.isFile && log.name[0] != '.') {
        const ext = path.extname(log.name);
        if (!setname || setname == path.basename(log.name, ext)) {
          if (!when || when == ext.substr(1)) {
            logs.push({ userid, log });
          }
        }
      } else if (!userid && log.isDirectory) {
        // if enumerating all logs in all user directories, then enumerate logs in this users
        // directory.
        await this.getLogs(setname, when, log.name, logs);
      }
    }
    return logs;
  }

  async getAllActiveHashes() {
    const hashes: Hashes = {};
    for (const logEntry of await this.getLogs()) {
      const log = logEntry.log;
      const ext = path.extname(log.name);
      switch (ext) {
      case '.current': break;		// ignore, just a link to newest entry
      case '.running':
        throw new Error("can't when a backup is running");
        break;
      default:
        const name = path.basename(log.name, ext);
        const instance = new BackupInstance({ target: this, setname: name, userid: logEntry.userid })
        await instance.getHashesFromInstanceLog(ext.substr(1), hashes);
        break;
      }
    }
    return hashes;
  }

  async _removeEntry(fn: string) {
    const l = this.filesDb.length;
    await fs.unlink(fn);
    fn = path.dirname(fn);
    do {
      try {
        await fs.rmdir(fn);
      } catch(e) {
        if (e.code == 'ENOTEMPTY') return;
        throw e;
      }
      fn = path.dirname(fn);
    } while (fn.length > l);
  }

  async _scanFs(root: string, callback: (root: string, entry: any, key: string) => Promise<void>) {
    const dir = await fs.readdir(root);
    const s = this.filesDb.length + 1;
    for (let i = 0; i < dir.length; i++) {
      const entry = dir[i];
      if (entry.isDirectory) {
        await this._scanFs(path.join(root, entry.name), callback);
      } else if (entry.isFile) {
        const key = this.filesystem.keyFromFile(root.substr(s), entry.name);
        await callback(root, entry, key);
      }
    }
  }

  async clean() {
    const hashes = await this.getAllActiveHashes();
    const stats = { cleaned: 0 };
    await this._scanFs(this.filesDb, async (root, entry, key) => {
      memtrack();
      if (key && !(key in hashes)) {
        console.log('REMOVE ' + key);
        stats.cleaned ++;
        await this._removeEntry(path.join(root, entry.name));
      }
    });
    console.log('Cleaned', stats.cleaned, 'orphaned hashes');
  }

  async getStats(setname: string, userid: string | null | undefined, when: string) {
    const instance = new BackupInstance({ target: this, userid, setname });
    const lines = await instance.getLinesFromInstanceLog(when);
    return lines.pop().stats;
  }

  async listFiles(
    setname: string,
    when: string,
    userid: string | null | undefined,
    filters: string[] | null | undefined,
    sources: BackupSources,
    log: ((s: string) => void) | null | undefined
  ) {
    function LOG(s: string) { (log||console.log)(s); }
    const instance = new BackupInstance({ target: this, setname, userid });
    const search = filters ? new Filter({ filters }) : null;
    for (const entry of await instance.getLinesFromInstanceLog(when)) {
      if (sources) {
        if (entry.type == 'SOURCE') {
          LOG(`${entry.root}`);
        }
      } else {
        switch(entry.type) {
        case 'F':
          if (search && !search.ignores(entry.path)) {
            LOG(`${entry.mtime} ${entry.uid||'-'}:${entry.gid||'-'} ${entry.mode} ${entry.size.padStart(10)} ${entry.path}`);
          }
          break;
        }
      }
    }
  }

  async listWhen(opts: ListOptions, when: Date | 'current' | 'running' | undefined) {
    const { log, filter, sources, since } = opts;
    let { setname, userid } = opts;
    function LOG(s: string) {
      (log||console.log)(s);
    }
    for await (const logEntry of await this.getLogs(setname, when == 'current' ? when : undefined, userid)) {
      const index = logEntry.log;
      let ext = path.extname(index.name);
      const name = index.name.substr(0, index.name.length - ext.length);
      if (name != setname) {
        LOG(`${logEntry.userid ? `User ID: ${logEntry.userid} ` : ''}Backup Set: ${name}`);
        setname = name;
      }
      ext = ext.substr(1);
      if (when == 'current') {
        if (ext == when) {
          await this.listFiles(name, when, userid, filter, sources, log);
          return;
        }
      } else {
        switch(ext) {
        case 'running': case 'current':
          break;
        default:
          const instance = new Date(`${BackupLog.ext2iso(ext)}`);
          if (when) {
            if ((<Date>when).getTime() == instance.getTime()) {
              await this.listFiles(name, ext, logEntry.userid, filter, sources, log);
              return;
            }
          } else {
            if (!since || instance.getTime() >= since.getTime()) {
              const stats = await this.getStats(name, logEntry.userid, ext);
              LOG(`${instance.toISOString()} ${stats.files} files ${((stats.bytes*100/1024/1024)|0)/100} MB took ${stats.took/1000} seconds`);
              if (sources) {
                await this.listFiles(name, ext, logEntry.userid, filter, sources, log);
              }
            }
          }
          break;
        }
      }
    }
  }

  async list(opts: ListOptions) {
    let { when } = opts;
    switch (when) {
      case 'current':
      case undefined: case null:
        await this.listWhen(opts, when);
        break;
      default:
        if (when.match(/^[0-9]{8}T[0-9]{9}Z$/)) {
          when = BackupLog.ext2iso(when);
        }
        await this.listWhen(opts, new Date(when));
        break;
    }
  }

  async backup({ backupset }: { backupset: BackupSet }) {
    const target = this;
    const instance = new BackupInstance({ target, setname: backupset.setname })
    await instance.createNewInstance();
    return await backupset.backupTo(instance);
  }

  async verify({ setname, when, userid, compare = false, compareWith, verbose = false, log }: VerifyOptions) {
    const target = this;
    if (setname) {
      const instance = new BackupInstance({ target, setname, userid })
      return await instance.verify({ setname, when, compare, compareWith, verbose, log });
    }
    return await this.fsck({ verbose });
  }

  async complete({ backupset }: { backupset: BackupSet }) {
    return await backupset.complete();
  }

  async restore(opts: any) {      // TODO do properly
    const { setname } = opts;
    const target = this;
    const instance = new BackupInstance({ target, setname })
    return await instance.restore(opts);
  }

  async fsck({ verbose = false }: { verbose?: boolean }) {
    const stats = {
      total: 0,
      verified: 0,
      damaged: 0,
      orphaned: 0,
      missing: 0,
    }
    memtrack();
    const hashes = await this.getAllActiveHashes();
    await this._scanFs(this.filesDb, async (root, entry, key) => {
      stats.total ++;
      if (key) {
        if (!(key in hashes)) {
          console.log('ORPHANED ' + key);
          stats.orphaned ++;
        } else {
          const parts = key.split('.');
          const hash: string = parts[0];
          const size: number = parseInt(parts[1]);
          try {
            await this.filesystem.verify(size, hash);
            memtrack();
            stats.verified ++;
            if (verbose) console.log('OK ' + key);
            hashes[key].seen = true;
          } catch(e) {
            stats.damaged ++;
            console.log('ERROR ' + key);
            console.dir(e);
          }
        }
      }
    });
    Object.keys(hashes).filter(key => !hashes[key].seen).forEach(key => {
      console.log('MISSING ' + key);
      stats.missing ++;
    });
    console.log(
      'Total', stats.total,
      'Verified', stats.verified,
      'Orphaned', stats.orphaned,
      'Damaged', stats.damaged,
      'Missing', stats.missing
    );
    memtrack();
  }

  destroy() {
  }
};
