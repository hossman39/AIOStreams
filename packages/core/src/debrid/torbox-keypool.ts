import { createLogger } from '../utils/index.js';

const logger = createLogger('torbox:keypool');

interface PoolKeyState {
  key: string;
  health: 'healthy' | 'rate_limited' | 'blocked';
  usageTimes: number[];
  lastErrorAt: number;
}

const ROLLING_WINDOW_MS = 60 * 60 * 1000; // 60 minutes
const RECOVERY_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes

function maskKey(key: string): string {
  if (key.length <= 8) return '***';
  return key.slice(0, 4) + '...' + key.slice(-4);
}

class TorboxKeyPool {
  private keys: PoolKeyState[];

  constructor(apiKeys: string[]) {
    this.keys = apiKeys.map((key) => ({
      key,
      health: 'healthy' as const,
      usageTimes: [],
      lastErrorAt: 0,
    }));
    logger.info(`Key pool initialized with ${this.keys.length} keys`);
  }

  selectKey(): string {
    const now = Date.now();

    // Try to find the healthy key with lowest rolling usage
    let best: PoolKeyState | null = null;
    let bestUsage = Infinity;

    for (const k of this.keys) {
      if (k.health !== 'healthy') {
        // Auto-recover after timeout
        if (now - k.lastErrorAt >= RECOVERY_TIMEOUT_MS) {
          logger.info(
            `Key auto-recovered: ${maskKey(k.key)} (was ${k.health})`
          );
          k.health = 'healthy';
        } else {
          continue;
        }
      }

      // Count usage in rolling window
      const cutoff = now - ROLLING_WINDOW_MS;
      k.usageTimes = k.usageTimes.filter((t) => t > cutoff);
      const usage = k.usageTimes.length;

      if (best === null || usage < bestUsage) {
        best = k;
        bestUsage = usage;
      }
    }

    if (best) {
      // Record usage immediately so the next call picks a different key
      best.usageTimes.push(now);
      logger.info(
        `Selected key: ${maskKey(best.key)} (usage: ${bestUsage}, total keys: ${this.keys.length})`
      );
      return best.key;
    }

    // All keys unhealthy - use the one that errored longest ago
    let oldest: PoolKeyState | null = null;
    for (const k of this.keys) {
      if (oldest === null || k.lastErrorAt < oldest.lastErrorAt) {
        oldest = k;
      }
    }

    if (oldest) {
      logger.warn(
        `All keys unhealthy, using oldest errored: ${maskKey(oldest.key)}`
      );
      return oldest.key;
    }

    return this.keys[0].key;
  }

  recordUsage(apiKey: string): void {
    const k = this.keys.find((k) => k.key === apiKey);
    if (k) {
      k.usageTimes.push(Date.now());
    }
  }

  recordError(apiKey: string, statusCode: number): void {
    const k = this.keys.find((k) => k.key === apiKey);
    if (!k) return;

    if (statusCode === 429) {
      k.health = 'rate_limited';
    } else if (statusCode === 401 || statusCode === 403) {
      k.health = 'blocked';
    } else {
      return;
    }
    k.lastErrorAt = Date.now();
    logger.warn(`Key marked ${k.health}: ${maskKey(k.key)} (HTTP ${statusCode})`);
  }

  hasKey(apiKey: string): boolean {
    return this.keys.some((k) => k.key === apiKey);
  }

  getKeyCount(): number {
    return this.keys.length;
  }
}

// Singleton instances keyed by the raw credential string
const instances = new Map<string, TorboxKeyPool>();

// Reverse lookup: individual key -> raw credential string that contains it
const keyToRawCredential = new Map<string, string>();

export function getKeyPool(rawCredential: string): TorboxKeyPool | null {
  if (!rawCredential || !rawCredential.includes(',')) {
    return null;
  }

  if (!instances.has(rawCredential)) {
    const keys = rawCredential
      .split(',')
      .map((k) => k.trim())
      .filter((k) => k.length > 0);
    if (keys.length < 2) return null;
    instances.set(rawCredential, new TorboxKeyPool(keys));
    // Register each individual key for reverse lookup
    for (const key of keys) {
      keyToRawCredential.set(key, rawCredential);
    }
  }

  return instances.get(rawCredential)!;
}

/**
 * Given an individual key that belongs to a pool, returns the raw
 * comma-separated credential string so a fresh key can be selected.
 */
export function getRawCredentialForKey(singleKey: string): string | null {
  return keyToRawCredential.get(singleKey) ?? null;
}

export function selectKeyFromPool(rawCredential: string): string {
  const pool = getKeyPool(rawCredential);
  if (!pool) return rawCredential;
  return pool.selectKey();
}

export function recordPoolUsage(
  rawCredential: string,
  usedKey: string
): void {
  const pool = getKeyPool(rawCredential);
  if (pool) pool.recordUsage(usedKey);
}

export function recordPoolError(
  rawCredential: string,
  usedKey: string,
  statusCode: number
): void {
  const pool = getKeyPool(rawCredential);
  if (pool) pool.recordError(usedKey, statusCode);
}
