
import * as path from 'https://deno.land/std@0.91.0/path/mod.ts';

export class FsFile {
  fn?: string | URL;
  stream?: Deno.File | Deno.Reader | Deno.Writer;
  opened?: boolean;
  encoder?: TextEncoder;
  public rid() { return this.opened && (<Deno.File>this.stream).rid; }
  public file() { return this.opened && (<Deno.File>this.stream); }
  public isFile() { return this.opened };
  public isStream() { return !this.opened && this.stream; };
  public reader() { return <Deno.Reader>this.stream; }
  public writer() { return <Deno.Writer>this.stream; }
  public close() { if (this.opened) (<Deno.File>this.stream).close(); }
  static async open(fn: string | URL | Deno.Reader | Deno.Writer, options: Deno.OpenOptions) {
    const handle = new FsFile();
    if (typeof fn == "string" || fn instanceof URL) {
      handle.fn = fn;
      handle.stream = await Deno.open(path.resolve(fn.toString()), options);
      handle.opened = true;
    } else {
      handle.stream = fn;
    }
    return handle;
  }
  public appendFile(line: string) {
    if (!this.opened) throw new Error('attempt to appendFile on a non-file stream');
    return this.appendString(line);
  }

  private _encode(str: string) {
    if (!this.encoder) this.encoder = new TextEncoder();
    return this.encoder.encode(str);
  }

  public appendString(line: string) {
    return this.append(this._encode(line));
  }

  public async append(data: Uint8Array) {
    if (this.opened) {
      const rid = (<Deno.File>this.stream).rid;
      await Deno.seek(rid, 0, Deno.SeekMode.End);
      await Deno.write(rid, data);
    } else {
      (<Deno.Writer>this.stream).write(data);
    }
  }

}
