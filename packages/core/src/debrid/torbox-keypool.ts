import { createLogger } from '../utils/index.js';

const logger = createLogger('torbox:keypool');

interface PoolKeyState {
  key: string;
  health: 'healthy' | 'rate_limited' | 'blocked';
  usageTimes: number[];
  lastErrorAt: number;
}

const ROLLING_WINDOW_MS = 60 * 60 * 1000; // 60 minutes
const EDGE_WINDOW_MS = 60 * 1000; // 1 minute (10/min edge limit)
const RECOVERY_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes
const HOURLY_LIMIT = 60;
const EDGE_LIMIT = 10;

function maskKey(key: string): string {
  if (key.length <= 8) return '***';
  return key.slice(0, 4) + '...' + key.slice(-4);
}

class TorboxKeyPool {
  private keys: PoolKeyState[];
  private roundRobinIndex: number = 0;
  private activeKey: string;

  constructor(apiKeys: string[]) {
    this.keys = apiKeys.map((key) => ({
      key,
      health: 'healthy' as const,
      usageTimes: [],
      lastErrorAt: 0,
    }));
    // Pre-select the first active key immediately
    this.activeKey = this.keys[0].key;
    logger.info(`Key pool initialized with ${this.keys.length} keys`);
  }

  /**
   * Return the pre-selected active key instantly (zero computation).
   * After returning, asynchronously advance to the next key.
   */
  getActiveKey(): string {
    const key = this.activeKey;

    // Record usage for the key we're handing out
    const k = this.keys.find((k) => k.key === key);
    if (k) k.usageTimes.push(Date.now());

    // Advance to next key off the hot path
    process.nextTick(() => this.advanceKey());

    return key;
  }

  /**
   * Compute and pre-select the next best key. Runs asynchronously,
   * never in the request path. All pruning, health checks, and
   * logging happen here.
   */
  private advanceKey(): void {
    const now = Date.now();
    const healthyKeys: PoolKeyState[] = [];
    const hourCutoff = now - ROLLING_WINDOW_MS;

    // Prune old usage times and recover unhealthy keys
    for (const k of this.keys) {
      k.usageTimes = k.usageTimes.filter((t) => t > hourCutoff);
      if (k.health !== 'healthy') {
        if (now - k.lastErrorAt >= RECOVERY_TIMEOUT_MS) {
          logger.info(
            `Key auto-recovered: ${maskKey(k.key)} (was ${k.health})`
          );
          k.health = 'healthy';
          healthyKeys.push(k);
        }
      } else {
        healthyKeys.push(k);
      }
    }

    if (healthyKeys.length > 0) {
      const minUsage = Math.min(
        ...healthyKeys.map((k) => k.usageTimes.length)
      );
      const candidates = healthyKeys.filter(
        (k) => k.usageTimes.length === minUsage
      );

      const pick = candidates[this.roundRobinIndex % candidates.length];
      this.roundRobinIndex = (this.roundRobinIndex + 1) % this.keys.length;

      this.activeKey = pick.key;
      logger.info(this.formatPoolStatus(now, pick.key));
      return;
    }

    // All keys unhealthy - use the one that errored longest ago
    let oldest: PoolKeyState | null = null;
    for (const k of this.keys) {
      if (oldest === null || k.lastErrorAt < oldest.lastErrorAt) {
        oldest = k;
      }
    }

    if (oldest) {
      this.activeKey = oldest.key;
      logger.warn(
        `All keys unhealthy, using oldest errored\n${this.formatPoolStatus(Date.now(), oldest.key)}`
      );
      return;
    }

    this.activeKey = this.keys[0].key;
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
    logger.warn(
      `Key marked ${k.health}: ${maskKey(k.key)} (HTTP ${statusCode})\n${this.formatPoolStatus(Date.now(), k.key)}`
    );
    // Immediately advance away from the errored key
    this.advanceKey();
  }

  hasKey(apiKey: string): boolean {
    return this.keys.some((k) => k.key === apiKey);
  }

  getKeyCount(): number {
    return this.keys.length;
  }

  private formatPoolStatus(now: number, selectedKey: string): string {
    const edgeCutoff = now - EDGE_WINDOW_MS;
    const hourCutoff = now - ROLLING_WINDOW_MS;
    const barLen = 10;

    const keyStats = this.keys.map((k) => {
      const hourUsage = k.usageTimes.filter((t) => t > hourCutoff).length;
      const edgeUsage = k.usageTimes.filter((t) => t > edgeCutoff).length;
      const hourFill = Math.round((hourUsage / HOURLY_LIMIT) * barLen);
      const edgeFill = Math.round((edgeUsage / EDGE_LIMIT) * barLen);
      const hourBar =
        '\u2588'.repeat(hourFill) + '\u2591'.repeat(barLen - hourFill);
      const edgeBar =
        '\u2588'.repeat(edgeFill) + '\u2591'.repeat(barLen - edgeFill);
      const marker = k.key === selectedKey ? '>' : ' ';
      const health = k.health !== 'healthy' ? ` [${k.health}]` : '';
      return `${marker}${maskKey(k.key)} 1h:${hourBar} ${hourUsage}/${HOURLY_LIMIT} | 1m:${edgeBar} ${edgeUsage}/${EDGE_LIMIT}${health}`;
    });

    return `Selected: ${maskKey(selectedKey)}\n` + keyStats.join('\n');
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

// Cache the last selection to deduplicate calls within the same stream request.
// Multiple addons (Comet, Torz, NZBHydra) all call selectKeyFromPool within
// milliseconds — they should all get the same key and only count as one usage.
const selectionCache = new Map<string, { key: string; expiresAt: number }>();
const SELECTION_CACHE_TTL_MS = 2000; // 2 seconds

/**
 * Return the pre-selected key from the pool instantly.
 * Key selection and logging happen asynchronously after the return.
 * Within a 2-second window, returns the same key to ensure all addons
 * in a single stream request use the same key and count as one usage.
 */
export function selectKeyFromPool(rawCredential: string): string {
  const pool = getKeyPool(rawCredential);
  if (!pool) return rawCredential;

  const now = Date.now();
  const cached = selectionCache.get(rawCredential);
  if (cached && now < cached.expiresAt) {
    return cached.key;
  }

  const key = pool.getActiveKey();
  selectionCache.set(rawCredential, {
    key,
    expiresAt: now + SELECTION_CACHE_TTL_MS,
  });
  return key;
}

export function recordPoolError(
  rawCredential: string,
  usedKey: string,
  statusCode: number
): void {
  const pool = getKeyPool(rawCredential);
  if (pool) pool.recordError(usedKey, statusCode);
}
