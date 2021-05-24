import * as fs from './fs.ts';
import ip from 'https://raw.githubusercontent.com/Mehuge/node-ip/master/lib/ip.ts';

interface AuthenticationDatabase {
  keys: {
    [key: string]: {
      userid: string;
      email?: string;
      allow?: string[];
      password?: string;
    }
  }
}

export default class {
  private path: string | null = null;
  private auth: AuthenticationDatabase | null = null;
  constructor({ path }: { path: string }) {
    this.path = path;
  }
  async load() {
    if (this.path && await fs.exists(this.path)) {
      this.auth = JSON.parse(await fs.readFile(this.path));
    } else {
      this.auth = null;
    }
  }
  exists() {
    return this.auth != null;
  }
  authenticate({ key, address }: { key: string, address: string }) {
    if (!this.auth) return;     // no auth-database, not authenticated
    const account = this.auth.keys[key];
    if (!account) return;       // invalid access key, not authenticated
    const allow = account.allow;
    if (allow && allow.length) {
      for (let i = 0; i < allow.length; i++) {
        let subnet = allow[i];
        if (subnet == address) return account;
        if (subnet.indexOf('/') == -1) subnet += '/32';
        if (ip.cidrSubnet(subnet).contains(address)) {
          return account;       // authenticated
        }
      }
      return;     // ip checks failed, not authenticated
    }
    return account;
  }
}
