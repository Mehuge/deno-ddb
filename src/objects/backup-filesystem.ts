import FileSystemV5 from './hash-filesystem-v5.ts';

export default class BackupFileSystem {

  private root: string | null = null;
  private hfs: FileSystemV5 | null = null;

  static ERROR: { [code: string]: string } = {
    SETLOCATION_NOT_CALLED: 'setLocation not called',
    NOT_FOUND: 'Not found',
    UNKNOWN_FILESYSTEM: 'unknown hash filesystem type',
    ENTRY_CORRUPT: 'files.db entry corrupt',
  };

  static assert(test: boolean, name: string) {
    if (!test) {
      const error = new Error();
      error.message = BackupFileSystem.ERROR[name];
      error.name = name;
      throw error;
    }
  }

  constructor() {
    this.root = null;
  }

  async setLocation(root: string, fstype = 'hash-v5') {
    this.root = root;
    switch(fstype) {
      case 'hash-v5':     /* like v4 but no variant, and folder names are based on hash */
        this.hfs = new FileSystemV5({ root });
        break;
      default:
        BackupFileSystem.assert(false, BackupFileSystem.ERROR.UNKNOWN_FILESYSTEM);
    }
  }

  getLocation() {
    return this.root;
  }

  /** Put a file in the backup store. */
  async put(file: string, size: number, hash: string, options: { compressed?: boolean } = { }) {
    let stored = true;
    const hfs = this.hfs;
    BackupFileSystem.assert(hfs != null, BackupFileSystem.ERROR.SETLOCATION_NOT_CALLED);
    if (hfs) {
      const key = hfs.getKey(hash, size);
      const exists = await hfs.exists(key);
      if (!exists) {
        await hfs.store(file, key, options.compressed || false);
      }
    }
    return { stored };
  }

  async verify(size: number, hash: string, compareWith?: string) {
    const hfs = this.hfs;
    BackupFileSystem.assert(hfs != null, BackupFileSystem.ERROR.SETLOCATION_NOT_CALLED);
    if (hfs) {
      const key = hfs.getKey(hash, size);
      const exists = await hfs.exists(key);
      BackupFileSystem.assert(exists, BackupFileSystem.ERROR.NOT_FOUND);
      const hash2 = await hfs.hashKey(key);
      BackupFileSystem.assert(hash2 == hash, BackupFileSystem.ERROR.ENTRY_CORRUPT);
      if (compareWith) await hfs.compare(key, compareWith);
    }
  }

  async hashFile(file: string) {
    const hfs = this.hfs;
    BackupFileSystem.assert(hfs != null, BackupFileSystem.ERROR.SETLOCATION_NOT_CALLED);
    if (hfs) {
      return await hfs.hashFile(file);
    }
  }

  async restore(size: number, hash: string, copyTo: string, isCompressedStream: boolean = false) {
    const hfs = this.hfs;
    BackupFileSystem.assert(hfs != null, BackupFileSystem.ERROR.SETLOCATION_NOT_CALLED);
    if (hfs) {
      await hfs.restore(hfs.getKey(hash, size), copyTo, isCompressedStream);
    }
  }

  async has(size: number, hash: string) {
    const hfs = this.hfs;
    BackupFileSystem.assert(hfs != null, BackupFileSystem.ERROR.SETLOCATION_NOT_CALLED);
    if (hfs) {
      const key = hfs.getKey(hash, size);
      return await hfs.exists(key);
    }
    return false;
  }

  keyFromFile(dir: string, name: string) {
    const hfs = this.hfs;
    BackupFileSystem.assert(hfs != null, BackupFileSystem.ERROR.SETLOCATION_NOT_CALLED);
    return (<FileSystemV5>hfs).keyFromFile(dir, name);
  }

  async get(hash: string) {
    // get a file from the backup store
    throw new Error('not implemented');
  }
}
