import { memstats } from './debug.ts';
import { BackupInstance } from './backup-instance.ts';
import { BackupSource } from './backup-source.ts';

export interface BackupStats {
  skipped: number;
  folders: number;
  files: number;
  bytes: number;
  backedUp: {
    files: number;
    bytes: number;
  }
}

export class BackupSet {
  private setname: string = 'default';
  private verbose: boolean = false;
  private sources: BackupSource[] = [];
  private started: Date | null = null;
  private instance: BackupInstance | null = null;
  private stats: BackupStats = { skipped: 0, folders: 0, files: 0, bytes: 0, backedUp: { files: 0, bytes: 0, } }
  constructor({ setname, sources, verbose = false }: { setname: string, sources: BackupSource[] | null, verbose?: boolean }) {
    if (setname) this.setname = setname;
    if (sources) this.sources = sources || [];
    this.verbose = verbose;
  }

  addSource(source: BackupSource) {
    this.sources.push(source);
  }

  getSources() {
    return this.sources;
  }

  async backupTo(instance: BackupInstance) {
    this.instance = instance;
    this.lastBackup = await instance.log().getLastBackup();
    this.started = new Date();
    for (const source of this.sources) {
      await source.backupTo(instance, this.stats, this.lastBackup);
    }
    this.stats.took = Date.now() - this.started.valueOf();
    await instance.log().finish('OK ' + JSON.stringify(this.stats));
  }

  async complete() {
    await this.instance.complete(this.started);
    this.displayStats();
  }

  displayStats() {
    const { skipped, folders, files, bytes, backedUp, took } = this.stats
    console.log(`Backup ${this.setname} complete. Took ${took/1000} seconds.`);
    console.log(`Processed: ${files} files (${((bytes+1023)/1024/1024)|0} MB) ${folders} folders. Skipped ${skipped}.`);
    console.log(`Backed up: ${backedUp.files} files (${((backedUp.bytes+1023)/1024/1024)|0} MB)`);
  }

  getStats() {
    return this.stats;
  }
};

export default BackupSet;
