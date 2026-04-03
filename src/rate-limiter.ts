/**
 * Dual-window sliding rate limiter.
 * Port of DualWindowRateLimiter from lol_ingest.py:180-228.
 *
 * Enforces two concurrent sliding windows (e.g., 490/10s + 29000/10min)
 * to stay within Riot production key limits with a safety margin.
 */
export class DualWindowRateLimiter {
  private maxShort: number;
  private windowShortMs: number;
  private maxLong: number;
  private windowLongMs: number;
  private tShort: number[] = [];
  private tLong: number[] = [];

  constructor(maxShort: number, windowShortS: number, maxLong: number, windowLongS: number) {
    this.maxShort = maxShort;
    this.windowShortMs = windowShortS * 1000;
    this.maxLong = maxLong;
    this.windowLongMs = windowLongS * 1000;
  }

  private purge(now: number): void {
    const cutShort = now - this.windowShortMs;
    while (this.tShort.length > 0 && this.tShort[0]! <= cutShort) this.tShort.shift();
    const cutLong = now - this.windowLongMs;
    while (this.tLong.length > 0 && this.tLong[0]! <= cutLong) this.tLong.shift();
  }

  async acquire(): Promise<void> {
    while (true) {
      const now = performance.now();
      this.purge(now);

      if (this.tShort.length < this.maxShort && this.tLong.length < this.maxLong) {
        this.tShort.push(now);
        this.tLong.push(now);
        return;
      }

      let wait = 10;
      if (this.tShort.length >= this.maxShort && this.tShort.length > 0) {
        wait = Math.max(wait, this.tShort[0]! + this.windowShortMs - now);
      }
      if (this.tLong.length >= this.maxLong && this.tLong.length > 0) {
        wait = Math.max(wait, this.tLong[0]! + this.windowLongMs - now);
      }

      await Bun.sleep(Math.ceil(wait));
    }
  }
}
