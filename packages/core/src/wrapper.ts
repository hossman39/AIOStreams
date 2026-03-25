import {
  Addon,
  AddonCatalog,
  AddonCatalogResponse,
  AddonCatalogResponseSchema,
  AddonCatalogSchema,
  CatalogResponse,
  CatalogResponseSchema,
  Manifest,
  ManifestSchema,
  Meta,
  ParsedMeta,
  MetaPreview,
  MetaPreviewSchema,
  MetaResponse,
  MetaResponseSchema,
  MetaSchema,
  ParsedStream,
  Resource,
  Stream,
  StreamResponse,
  StreamResponseSchema,
  StreamSchema,
  Subtitle,
  SubtitleResponse,
  SubtitleResponseSchema,
  SubtitleSchema,
  ParsedMetaSchema,
} from './db/schemas.js';
import {
  Cache,
  makeRequest,
  createLogger,
  constants,
  maskSensitiveInfo,
  makeUrlLogSafe,
  formatZodError,
  PossibleRecursiveRequestError,
  Env,
  getTimeTakenSincePoint,
  RequestOptions,
} from './utils/index.js';
import { selectKeyFromPool, getRawCredentialForKey } from './debrid/torbox-keypool.js';
import { Preset, PresetManager } from './presets/index.js';
import { z } from 'zod';

const logger = createLogger('wrappers');

const manifestCache = Cache.getInstance<string, Manifest>(
  'manifest',
  Env.MANIFEST_CACHE_MAX_SIZE || Env.DEFAULT_MAX_CACHE_SIZE
);
const catalogCache = Cache.getInstance<string, MetaPreview[]>(
  'catalog',
  Env.CATALOG_CACHE_MAX_SIZE || Env.DEFAULT_MAX_CACHE_SIZE
);
const metaCache = Cache.getInstance<string, Meta>(
  'meta',
  Env.META_CACHE_MAX_SIZE || Env.DEFAULT_MAX_CACHE_SIZE
);
const subtitlesCache = Cache.getInstance<string, Subtitle[]>(
  'subtitles',
  Env.SUBTITLE_CACHE_MAX_SIZE || Env.DEFAULT_MAX_CACHE_SIZE
);
const addonCatalogCache = Cache.getInstance<string, AddonCatalog[]>(
  'addon_catalog',
  Env.ADDON_CATALOG_CACHE_MAX_SIZE || Env.DEFAULT_MAX_CACHE_SIZE
);
const streamsCache = Cache.getInstance<string, ParsedStream[]>(
  'streams',
  Env.STREAM_CACHE_MAX_SIZE || Env.DEFAULT_MAX_CACHE_SIZE
);

/**
 * Resolves TTL value from a cacheTtls map based on priority:
 * 1. presetId match
 * 2. hostname match (from manifestUrl)
 * 3. wildcard match (*)
 */
function resolveTtl(
  ttlMap: Record<string, number>,
  presetId?: string,
  manifestUrl?: string
): number {
  let resolvedTtl = undefined;
  let hostname: string | undefined;
  try {
    if (manifestUrl) {
      hostname = new URL(manifestUrl).hostname;
    }
  } catch {}

  if (presetId && ttlMap[presetId] !== undefined) {
    resolvedTtl = ttlMap[presetId];
  }

  if (resolvedTtl === undefined && hostname && ttlMap[hostname] !== undefined) {
    resolvedTtl = ttlMap[hostname];
  }

  if (resolvedTtl === undefined && ttlMap['*'] !== undefined) {
    resolvedTtl = ttlMap['*'];
  }
  return resolvedTtl !== undefined ? resolvedTtl : -1;
}

type ResourceParams = {
  type: string;
  id: string;
  extras?: string;
};

export class Wrapper {
  private readonly baseUrl: string;
  private readonly addon: Addon;
  private readonly manifestUrl: string;
  private readonly preset: typeof Preset;
  private rotatedBaseUrl: string | null = null;

  constructor(addon: Addon) {
    this.addon = addon;
    this.manifestUrl = this.addon.manifestUrl.replace('stremio://', 'https://');
    this.baseUrl = this.manifestUrl.split('/').slice(0, -1).join('/');
    this.preset = PresetManager.fromId(this.addon.preset.type);
  }

  /**
   * Validates an array of items against a schema, filtering out invalid ones
   * @param data The data to validate
   * @param schema The Zod schema to validate against
   * @param resourceName Name of the resource for error messages
   * @returns Array of validated items
   * @throws Error if all items are invalid
   */
  private validateArray<T>(
    data: unknown,
    schema: z.ZodSchema<T>,
    resourceName: string
  ): T[] {
    if (!Array.isArray(data)) {
      throw new Error(`${resourceName} is not an array`);
    }

    if (data.length === 0) {
      // empty array is valid
      return [];
    }

    const validItems = data
      .map((item) => {
        const parsed = schema.safeParse(item);
        if (!parsed.success) {
          logger.error(
            `An item in the response for ${resourceName} was invalid, filtering it out: ${formatZodError(parsed.error)}`
          );
          return null;
        }
        return parsed.data;
      })
      .filter((item): item is T => item !== null);

    if (validItems.length === 0) {
      throw new Error(`No valid ${resourceName} found`);
    }

    return validItems;
  }

  async getManifest(options?: {
    timeout?: number;
    bypassCache?: boolean;
  }): Promise<Manifest> {
    const cacheKey =
      this.preset.getCacheKey({
        resource: 'manifest',
        type: 'manifest',
        id: 'manifest',
        options: this.addon.preset.options,
      }) || this.manifestUrl;

    const requestFn = async (): Promise<Manifest> => {
      logger.debug(
        `Fetching manifest for ${this.addon.name} ${this.addon.displayIdentifier || this.addon.identifier} (${makeUrlLogSafe(this.manifestUrl)})`
      );
      try {
        const backgroundTimeout =
          Env.BACKGROUND_RESOURCE_REQUEST_TIMEOUT ?? Env.MAX_TIMEOUT;
        const res = await makeRequest(this.manifestUrl, {
          timeout: backgroundTimeout,
          headers: this.addon.headers,
          forwardIp: this.addon.ip,
        });
        if (!res.ok) {
          throw new Error(`${res.status} - ${res.statusText}`);
        }
        const data = await res.json();
        const manifest = ManifestSchema.safeParse(data);
        if (!manifest.success) {
          logger.error(`Manifest response was unexpected`);
          logger.error(formatZodError(manifest.error));
          logger.error(JSON.stringify(data, null, 2));
          throw new Error(
            `Manifest response could not be parsed: ${formatZodError(manifest.error)}`
          );
        }
        return manifest.data;
      } catch (error: any) {
        logger.error(
          `Failed to fetch manifest for ${this.getAddonName(this.addon)}: ${error.message}`
        );
        if (error instanceof PossibleRecursiveRequestError) {
          throw error;
        }
        throw new Error(
          `Failed to fetch manifest for ${this.getAddonName(this.addon)}: ${error.message}`
        );
      }
    };

    return this._request({
      requestFn,
      timeout: options?.timeout ?? Env.MANIFEST_TIMEOUT,
      resourceName: 'manifest',
      cacher: manifestCache,
      cacheKey,
      cacheTtl: resolveTtl(
        Env.MANIFEST_CACHE_TTL,
        this.addon.preset.type,
        this.manifestUrl
      ),
      bypassCache: options?.bypassCache,
    });
  }

  async getStreams(type: string, id: string): Promise<ParsedStream[]> {
    // Select a fresh key for this stream request (one key per request)
    this.rotatedBaseUrl = this.getRotatedBaseUrl();

    const validator = (data: any): Stream[] => {
      return this.validateArray(data.streams, StreamSchema, 'streams');
    };

    const cacheKey =
      this.preset.getCacheKey({
        resource: 'stream',
        type,
        id,
        options: this.addon.preset.options,
      }) || this.buildResourceUrl('stream', type, id);
    const streamTtl = resolveTtl(
      Env.STREAM_CACHE_TTL,
      this.addon.preset.type,
      this.manifestUrl
    );
    const streams = await this.makeResourceRequest(
      'stream',
      { type, id },
      this.addon.timeout,
      validator,
      streamTtl != -1 ? streamsCache : undefined,
      streamTtl,
      this.preset.getCacheKey({
        resource: 'stream',
        type,
        id,
        options: this.addon.preset.options,
      })
    );
    const start = Date.now();
    const parser = new (this.preset.getParser())(this.addon);
    let invalidateCache: boolean = false;
    try {
      const parsedStreams = streams
        .flatMap((stream: Stream) => parser.parse(stream))
        .filter((stream: any) => !stream.skip);
      if (parsedStreams.every((stream) => 'skip' in stream || stream.error)) {
        invalidateCache = true;
      }
      logger.debug(
        `Parsed ${parsedStreams.length} streams for ${this.getAddonName(this.addon)} in ${getTimeTakenSincePoint(start)}`
      );
      return parsedStreams as ParsedStream[];
    } catch (error) {
      invalidateCache = true;
      throw error;
    } finally {
      if (invalidateCache) {
        logger.debug(
          `Invalidating cache entry for ${this.getAddonName(this.addon)}`
        );
        streamsCache
          .delete(cacheKey)
          .catch((error) =>
            logger.error(
              `Failed to invalidate cache entry: ${error instanceof Error ? error.message : error}`
            )
          );
      }
    }
  }

  async getCatalog(
    type: string,
    id: string,
    extras?: string
  ): Promise<MetaPreview[]> {
    const validator = (data: any): MetaPreview[] => {
      return this.validateArray(data.metas, MetaPreviewSchema, 'catalog items');
    };

    const catalogTtl = resolveTtl(
      Env.CATALOG_CACHE_TTL,
      this.addon.preset.type,
      this.manifestUrl
    );
    return await this.makeResourceRequest(
      'catalog',
      { type, id, extras },
      Env.CATALOG_TIMEOUT,
      validator,
      catalogTtl != -1 ? catalogCache : undefined,
      catalogTtl,
      this.preset.getCacheKey({
        resource: 'catalog',
        type,
        id,
        options: this.addon.preset.options,
        extras,
      })
    );
  }

  async getMeta(type: string, id: string): Promise<ParsedMeta> {
    const validator = (data: any): Meta => {
      const parsed = MetaSchema.safeParse(data.meta);
      if (!parsed.success) {
        logger.error(formatZodError(parsed.error));
        throw new Error(
          `Failed to parse meta for ${this.getAddonName(this.addon)}`
        );
      }
      return parsed.data;
    };
    const metaTtl = resolveTtl(
      Env.META_CACHE_TTL,
      this.addon.preset.type,
      this.manifestUrl
    );
    const meta: Meta = await this.makeResourceRequest(
      'meta',
      { type, id },
      Env.META_TIMEOUT,
      validator,
      metaTtl != -1 ? metaCache : undefined,
      metaTtl,
      this.preset.getCacheKey({
        resource: 'meta',
        type,
        id,
        options: this.addon.preset.options,
      })
    );
    // parse streams in meta.videos.streams if present
    const parser = new (this.preset.getParser())(this.addon);
    if (meta.videos) {
      meta.videos = meta.videos.map((video) => {
        const parsedStreams = video.streams
          ?.map((stream) => parser.parse(stream))
          .filter((stream) => ('skip' in stream ? !stream.skip : true));
        if (parsedStreams) {
          video.streams = parsedStreams as ParsedStream[];
        }
        return video;
      });
    }
    return ParsedMetaSchema.parse(meta);
  }

  async getSubtitles(
    type: string,
    id: string,
    extras?: string
  ): Promise<Subtitle[]> {
    const validator = (data: any): Subtitle[] => {
      return this.validateArray(data.subtitles, SubtitleSchema, 'subtitles');
    };

    const subtitleTtl = resolveTtl(
      Env.SUBTITLE_CACHE_TTL,
      this.addon.preset.type,
      this.manifestUrl
    );
    return await this.makeResourceRequest(
      'subtitles',
      { type, id, extras },
      this.addon.timeout,
      validator,
      subtitleTtl != -1 ? subtitlesCache : undefined,
      subtitleTtl,
      this.preset.getCacheKey({
        resource: 'subtitles',
        type,
        id,
        options: this.addon.preset.options,
      })
    );
  }

  async getAddonCatalog(type: string, id: string): Promise<AddonCatalog[]> {
    const validator = (data: any): AddonCatalog[] => {
      return this.validateArray(
        data.addons,
        AddonCatalogSchema,
        'addon catalog items'
      );
    };

    const addonCatalogTtl = resolveTtl(
      Env.ADDON_CATALOG_CACHE_TTL,
      this.addon.preset.type,
      this.manifestUrl
    );
    return await this.makeResourceRequest(
      'addon_catalog',
      { type, id },
      Env.CATALOG_TIMEOUT,
      validator,
      addonCatalogTtl != -1 ? addonCatalogCache : undefined,
      addonCatalogTtl,
      this.preset.getCacheKey({
        resource: 'addon_catalog',
        type,
        id,
        options: this.addon.preset.options,
      })
    );
  }

  async makeRequest(url: string, options: RequestOptions) {
    return await makeRequest(url, {
      headers: this.addon.headers,
      forwardIp: this.addon.ip,
      ...options,
    });
  }

  private async _request<T>(options: {
    requestFn: () => Promise<T>;
    timeout: number;
    resourceName: string;
    cacher?: Cache<string, T>;
    cacheKey: string;
    cacheTtl: number;
    shouldCache?: (data: T) => boolean;
    bypassCache?: boolean;
  }): Promise<T> {
    const {
      requestFn,
      timeout,
      resourceName,
      cacher,
      cacheKey,
      cacheTtl,
      shouldCache,
      bypassCache,
    } = options;

    let doBackground = Env.BACKGROUND_RESOURCE_REQUESTS_ENABLED && cacher;

    let cached = null;

    if (cacher) {
      cached = await cacher.get(cacheKey);
      if (cached && !bypassCache) {
        logger.debug(
          `Returning cached ${resourceName} for ${this.getAddonName(this.addon)}`
        );
        return cached;
      }
    }

    const processRequest = async () => {
      const result = await requestFn();
      const doCache = shouldCache ? shouldCache(result) : true;
      // bypass cache only skips retrieving from cache, it still caches the result
      if (cacher && doCache) {
        await cacher.set(cacheKey, result, cacheTtl);
      }
      return result;
    };

    const requestPromise = processRequest();

    if (!doBackground) {
      return await requestPromise;
    }

    const timeoutPromise: Promise<T> = new Promise<T>((_, reject) =>
      setTimeout(
        () =>
          reject(
            new Error(
              `Request for ${resourceName} for ${this.getAddonName(this.addon)} timed out after ${timeout}ms`
            )
          ),
        timeout
      )
    );

    try {
      return await Promise.race([requestPromise, timeoutPromise]);
    } catch (error: any) {
      if (cached) {
        logger.warn(
          `Returning cached ${resourceName} for ${this.getAddonName(this.addon)} after request failure: ${error.message}`
        );
        return cached;
      }
      if (error.message.includes('timed out')) {
        logger.warn(
          `Request for ${resourceName} for ${this.getAddonName(this.addon)} timed out. Will process in background.`
        );
        requestPromise.catch((bgError) => {
          logger.warn(
            `Background request for ${resourceName} for ${this.getAddonName(this.addon)} failed: ${bgError.message}`
          );
        });
      }
      throw error;
    }
  }

  private async makeResourceRequest<T>(
    resource: Resource,
    params: ResourceParams,
    timeout: number,
    validator: (data: unknown) => T,
    cacher: Cache<string, T> | undefined,
    cacheTtl: number,
    cacheKey?: string
  ) {
    const { type, id, extras } = params;
    const url = this.buildResourceUrl(resource, type, id, extras);
    const effectiveCacheKey = cacheKey || url;
    let doBackground = Env.BACKGROUND_RESOURCE_REQUESTS_ENABLED && cacher;

    logger.info(
      `Fetching ${resource} of type ${type} with id ${id} and extras ${extras} (${makeUrlLogSafe(url)})`,
      {
        cacheKey: cacheKey ? makeUrlLogSafe(cacheKey) : undefined,
      }
    );

    const requestFn = async (): Promise<T> => {
      try {
        const timeout = doBackground
          ? (Env.BACKGROUND_RESOURCE_REQUEST_TIMEOUT ?? Env.MAX_TIMEOUT)
          : this.addon.timeout;
        const res = await makeRequest(url, {
          timeout: timeout,
          headers: this.addon.headers,
          forwardIp: this.addon.ip,
        });

        if (!res.ok) {
          logger.error(
            `Failed to fetch ${resource} resource for ${this.getAddonName(this.addon)}: ${res.status} - ${res.statusText}`
          );
          throw new Error(`${res.status} - ${res.statusText}`);
        }

        const data: unknown = await res.json();
        return validator(data);
      } catch (error: any) {
        logger.error(
          `Failed to fetch ${resource} resource for ${this.getAddonName(this.addon)}: ${error.message}`
        );
        throw error;
      }
    };

    return this._request({
      requestFn,
      timeout,
      resourceName: resource,
      cacher,
      cacheKey: effectiveCacheKey,
      cacheTtl,
      shouldCache: (data: T) =>
        resource !== 'stream' || (Array.isArray(data) && data.length > 0),
    });
  }

  private buildResourceUrl(
    resource: Resource,
    type: string,
    id: string,
    extras?: string
  ): string {
    const extrasPath = extras ? `/${extras}` : '';
    const queryParams = new URL(this.manifestUrl).search;
    const baseUrl = this.rotatedBaseUrl ?? this.baseUrl;
    return `${baseUrl}/${resource}/${type}/${encodeURIComponent(id)}${extrasPath}.json${queryParams ? `?${queryParams.slice(1)}` : ''}`;
  }

  /**
   * Returns baseUrl with the torbox API key rotated if this addon uses
   * a multi-key pool. This ensures each stream request can use a different
   * key rather than always reusing the one baked in at config-generation time.
   */
  private getRotatedBaseUrl(): string {
    try {
      const urlObj = new URL(this.baseUrl);
      const pathParts = urlObj.pathname.split('/').filter((p) => p.length > 0);

      for (let i = 0; i < pathParts.length; i++) {
        const segment = pathParts[i];
        // Skip segments too short to be a base64-encoded config
        if (segment.length < 20) continue;

        const decoded = this.tryDecodeBase64Json(segment);
        if (decoded === null) continue;

        const config = JSON.parse(decoded);
        let modified = false;

        // Comet format: { debridServices: [{ service: "torbox", apiKey: "KEY" }] }
        if (Array.isArray(config.debridServices)) {
          for (const svc of config.debridServices) {
            if (svc.service === 'torbox' && svc.apiKey) {
              const rawCredential = getRawCredentialForKey(svc.apiKey);
              if (rawCredential) {
                const newKey = selectKeyFromPool(rawCredential);
                if (newKey !== svc.apiKey) {
                  svc.apiKey = newKey;
                  modified = true;
                }
              }
            }
          }
        }

        // StremThru Torz format: { stores: [{ c: "tb", t: "KEY" }] }
        if (Array.isArray(config.stores)) {
          for (const store of config.stores) {
            if (store.c === 'tb' && store.t) {
              const rawCredential = getRawCredentialForKey(store.t);
              if (rawCredential) {
                const newKey = selectKeyFromPool(rawCredential);
                if (newKey !== store.t) {
                  store.t = newKey;
                  modified = true;
                }
              }
            }
          }
        }

        if (modified) {
          // Re-encode using the same encoding as the original segment
          const newSegment = Buffer.from(JSON.stringify(config)).toString(
            'base64'
          );
          pathParts[i] = newSegment;
          urlObj.pathname = '/' + pathParts.join('/');
          return urlObj.toString();
        }
      }

      // Also handle Torbox addon format: raw API key as a path segment
      // URL: https://host/{apiKey}/manifest.json -> baseUrl is https://host/{apiKey}
      // The key is the last path segment in baseUrl
      if (pathParts.length > 0) {
        const lastSegment = pathParts[pathParts.length - 1];
        const rawCredential = getRawCredentialForKey(lastSegment);
        if (rawCredential) {
          const newKey = selectKeyFromPool(rawCredential);
          if (newKey !== lastSegment) {
            pathParts[pathParts.length - 1] = newKey;
            urlObj.pathname = '/' + pathParts.join('/');
            return urlObj.toString();
          }
        }
      }
    } catch {
      // If anything fails, silently return the original baseUrl
    }
    return this.baseUrl;
  }

  /**
   * Attempts to decode a URL path segment as base64-encoded JSON.
   * Tries standard base64 first, then URL-safe base64.
   * Returns the decoded string if valid JSON, or null otherwise.
   */
  private tryDecodeBase64Json(segment: string): string | null {
    // Try standard base64 first
    try {
      const decoded = Buffer.from(segment, 'base64').toString('utf-8');
      JSON.parse(decoded);
      return decoded;
    } catch {
      // ignore
    }
    // Try URL-safe base64
    try {
      const padding = 4 - (segment.length % 4);
      const padded = padding !== 4 ? segment + '='.repeat(padding) : segment;
      const decoded = Buffer.from(
        padded.replace(/-/g, '+').replace(/_/g, '/'),
        'base64'
      ).toString('utf-8');
      JSON.parse(decoded);
      return decoded;
    } catch {
      return null;
    }
  }

  private getAddonName(addon: Addon): string {
    return `${addon.name}${addon.displayIdentifier || addon.identifier ? ` ${addon.displayIdentifier || addon.identifier}` : ''}`;
  }
}
