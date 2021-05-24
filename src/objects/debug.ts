
const KB = 1 << 10;
const MB = KB << 10;
const GB = MB << 10;

const memoryUsage: { peak: number, low: number | undefined } = {
  peak: 0,
  low: undefined,
}

export function memtrack() {
  const used = memused();
  if (used > memoryUsage.peak) memoryUsage.peak = used;
  if (memoryUsage.low == undefined || used < memoryUsage.low) memoryUsage.low = used;
}

export function memstats() {
  console.log('MEM: Heap Usage', 'Min', F(<number>memoryUsage.low|0), 'Max', F(memoryUsage.peak));
  memdbg();
}

function F(b: number) {
  if (b > GB) return (b/GB).toFixed(1)+'/GiB';
  if (b > MB) return (b/MB).toFixed(1)+'/MiB';
  if (b > KB) return (b/KB).toFixed(1)+'/KiB';
  if (b > 1) return b + " bytes";
  return "1 byte";
}

export function memdbg() {
  // not supported
  console.log(
    'MEM:',
    'Process Total', F(0),
    'Heap Total', F(0),
    'External', F(0)
  );
}

export function memused() {
  // not supported
  return 0;
}


