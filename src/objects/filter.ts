
interface FilterItem {
  type: string;
  pattern: string[];
  re: RegExp;
}

export default class Filter {
  private filters: FilterItem[] | null = null;

  constructor({ filters, filterItems }: { filters?: string[], filterItems?: FilterItem[] | null }) {
    if (filters) {
      this._init(filters);
    } else if (filterItems) {
      this.filters = [...filterItems];
    }
  }

  getFilters() {
    return this.filters;
  }

  _init(patterns: string[]) {
    function re(patterns: string[]) {
      const res: FilterItem[] = [];
      for (let i = 0; i < patterns.length; i++) {
        const pattern = patterns[i];
        const type = pattern[0];
        const p = pattern.substr(1);
        // Handle special **/something case, which means any level of folder, including root
        // so matches both ^something and ^.+/something
        if (p.substr(0,2) == '**' && (p[2] == '/' || p[2] == '\\')) {
          res.push(...re([ type + p.substr(3) ]));
        }
        res.push({
          type,
          pattern: pattern.split(/[\\/]/g),
          re: new RegExp(
            '^' + p
            .replace(/\*\*[\\/]/g, '{STARSTARGLOBSLASH}')
            .replace(/\*\*/g, '{STARSTARGLOB}')
            .replace(/[.]/g,'\.')
            .replace(/[/\\]/g,'[/\\\\]')
            .replace(/\*/g, '[^/\\\\]*')
            .replace(/{STARSTARGLOB}/g, '.*')
            .replace(/{STARSTARGLOBSLASH}/g, '.+[\\\\/]')
          )
        });
      }
      return res;
    }
    this.filters = re(patterns);
  }

  ignores(str: string) {
    let ignored = null;
    if (this.filters) {
      for (const filter of this.filters) {
        if (filter.re.test(str)) {
          ignored = filter.type == '-' ? filter : null;
        }
      }
    }
    return ignored;
  }
}
