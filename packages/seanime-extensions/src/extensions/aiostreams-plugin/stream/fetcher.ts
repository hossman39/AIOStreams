import {
  AIOStreamsAPI,
  ParsedId,
  parseManifestUrl,
} from '../../../lib/aiostreams';
import {
  applyPreferredMapping,
  formatIdForSearch,
} from '../../../lib/aiostreams-resolver';
import { parseStremioId } from '../../../lib/stremio-id';
import { ResultsPanel } from '../results-panel';
import { StreamCache } from './cache';
import { StreamPlayer } from './player';
import { toStreamResult } from './mapping';
import { Context, LookupInfo, StreamResult, WebviewState } from '../types';

export type SearchIdPref = 'imdbId' | 'kitsuId' | 'anilistId';

function buildLookup(
  originalId: ParsedId,
  parsedId: ParsedId,
  anime: $app.AL_BaseAnime,
  mediaType: string
): LookupInfo {
  const fmt = (id: ParsedId, suffix: string) =>
    `${id.type}: ${id.value}${id.season !== undefined ? ` · S${id.season}` : ''}${id.episode !== undefined ? ` · E${id.episode}` : ''}${suffix}`;

  const lookup: LookupInfo = {
    original: fmt(originalId, anime.format ? ` (${anime.format})` : ''),
    resolved: fmt(parsedId, mediaType ? ` (${mediaType})` : ''),
    stremioId: `${formatIdForSearch(parsedId)}${parsedId.season !== undefined ? `:${parsedId.season}` : ''}${parsedId.episode !== undefined ? `:${parsedId.episode}` : ''}`,
  };
  if (parsedId.type === 'stremioId') lookup.resolved = '—';
  return lookup;
}

function parseStremioCustomSourceId(
  siteUrl: string,
  episodeNumber: number,
  aniDBEpisode: string | undefined
): { parsedId: ParsedId; mediaType: string } | null {
  const parts = siteUrl.split('|');
  if (parts.length !== 3) return null;
  try {
    const decoded = CryptoJS.enc.Utf8.stringify(
      CryptoJS.enc.Base64.parse(parts[2])
    );
    const parsed = JSON.parse(decoded);
    if (!parsed || typeof parsed !== 'object') return null;

    const mediaType =
      typeof parsed.type === 'string' && parsed.type ? parsed.type : 'series';

    let epMapping: unknown;
    if (parsed.episodes && typeof parsed.episodes === 'object') {
      if (aniDBEpisode && aniDBEpisode in parsed.episodes) {
        epMapping = parsed.episodes[aniDBEpisode];
      } else {
        epMapping = parsed.episodes[String(episodeNumber)];
      }
    }
    if (typeof epMapping === 'string' && epMapping) {
      // Only parse the id when it uses a scheme parseStremioId actually
      // understands. For arbitrary addon-defined ids, a trailing `:N` could
      // just be a part of the id, not a season/episode.
      const isKnownScheme =
        /^tt\d+/.test(epMapping) ||
        /^(kitsu|mal|anilist|tmdb|tvdb|anidb|simkl):/.test(epMapping);
      if (isKnownScheme) {
        const parsedStremioId = parseStremioId(epMapping);
        return {
          parsedId: {
            type: 'stremioId',
            value: parsedStremioId?.baseId ?? epMapping,
            season: parsedStremioId?.season,
            episode: parsedStremioId?.episode,
          },
          mediaType,
        };
      }
      return {
        parsedId: { type: 'stremioId', value: epMapping },
        mediaType,
      };
    }

    // No per-episode mapping (single entry meta) — use the meta's own id.
    return {
      parsedId: { type: 'stremioId', value: parsed.imdb_id || parsed.id },
      mediaType,
    };
  } catch (err) {
    $debug.warn(
      'Failed to parse custom source ID, falling back to AniList ID',
      err
    );
    return null;
  }
}

export class StreamFetcher {
  lastCacheKey: string | null = null;

  constructor(
    private readonly ctx: Context,
    private readonly panel: ResultsPanel,
    private readonly player: StreamPlayer,
    private readonly cache: StreamCache,
    private readonly pendingAnime: $ui.State<$app.AL_BaseAnime | null>,
    private readonly pendingEp: $ui.State<$app.Anime_Episode | number | null>,
    private readonly setSessionId: () => string,
    private readonly resetDownloadSession: () => void
  ) {}

  async fetch(
    anime: $app.AL_BaseAnime,
    episode: $app.Anime_Episode | number
  ): Promise<void> {
    const episodeNumber =
      typeof episode === 'number' ? episode : episode.episodeNumber;
    const aniDBEpisode =
      typeof episode === 'object' ? episode.aniDBEpisode : undefined;
    const manifestUrl = $getUserPreference('manifestUrl') ?? '';
    const searchId = ($getUserPreference('searchId') ??
      'imdbId') as SearchIdPref;

    let creds: ReturnType<typeof parseManifestUrl>;
    try {
      creds = parseManifestUrl(manifestUrl);
    } catch (err) {
      console.warn('AIOStreams: manifest URL invalid/missing', err);
      this.ctx.toast.error(
        'AIOStreams manifest URL is invalid or missing. Configure it in the extension settings.'
      );
      return;
    }
    const api = new AIOStreamsAPI(
      creds.baseUrl,
      creds.uuid,
      creds.encryptedPassword
    );

    $debug.info('AIOStreams: fetching streams', {
      animeId: anime.id,
      episodeNumber,
      searchId,
      format: anime.format ?? null,
    });
    $debug.debug('AIOStreams: anime details', anime);
    $debug.debug('AIOStreams: episode details', episode);

    const animeTitle = anime.title?.userPreferred ?? 'Unknown';
    const isMovie = String(anime.format ?? '').toUpperCase() === 'MOVIE';
    const episodeInfo = isMovie
      ? animeTitle
      : `${animeTitle} \xb7 Episode ${episodeNumber}`;

    this.pendingAnime.set(anime);
    this.pendingEp.set(episode);

    const autoPlay = this.ctx.preferences.playback.autoPlayFirstStream;
    const sessionId = this.setSessionId();
    this.resetDownloadSession();

    this.panel.wvState.set({
      results: [],
      loading: true,
      error: null,
      episodeInfo,
      timeTakenMs: null,
      animeLookupMs: null,
      searchMs: null,
      fromCache: false,
      errors: [],
      statistics: [],
      lookup: null,
      sessionId,
      autoPlay,
    });
    this.panel.show();

    const startTime = Date.now();
    let animeLookupMs: number | null = null;
    let searchMs: number | null = null;

    let parsedId: ParsedId | null = null;
    let mediaType: string | null = null;
    if (anime.siteUrl?.startsWith('ext_custom_source_stremio-custom-source')) {
      const parsed = parseStremioCustomSourceId(
        anime.siteUrl,
        episodeNumber,
        aniDBEpisode
      );
      if (parsed) {
        parsedId = parsed.parsedId;
        mediaType = parsed.mediaType;
      }
    }
    if (!parsedId) {
      parsedId = {
        type: 'anilistId',
        value: String(anime.id),
        episode: isMovie ? undefined : episodeNumber,
      };
    }
    mediaType = mediaType ?? (isMovie ? 'movie' : 'series');
    const originalId = { ...parsedId };

    if (parsedId.type !== 'stremioId') {
      const animeLookupStart = Date.now();
      try {
        const animeEntry = await api.anime('anilistId', anime.id);
        animeLookupMs = Date.now() - animeLookupStart;
        if (animeEntry) {
          applyPreferredMapping(parsedId, animeEntry, searchId);
          if (isMovie) {
            parsedId.season = undefined;
            parsedId.episode = undefined;
          }
          $debug.debug('AIOStreams: resolved id mapping', {
            originalId,
            mappedId: parsedId,
            animeLookupMs,
          });
        }
      } catch (err) {
        animeLookupMs = Date.now() - animeLookupStart;
        $debug.warn(
          'Failed to fetch anime details from AIOStreams, falling back to AniList ID search',
          err
        );
      }
    }

    const lookup = buildLookup(originalId, parsedId, anime, mediaType);

    const cacheKey = StreamCache.keyFor(parsedId);
    this.lastCacheKey = cacheKey;

    const cachedResults = this.cache.get(cacheKey);
    if (cachedResults) {
      $debug.info('AIOStreams: cache hit', {
        cacheKey,
        count: cachedResults.length,
      });
      this.applyResultsToPanel({
        results: cachedResults,
        loading: false,
        error: null,
        episodeInfo,
        timeTakenMs: Date.now() - startTime,
        animeLookupMs,
        searchMs: null,
        fromCache: true,
        errors: [],
        statistics: [],
        lookup,
        sessionId,
        autoPlay: autoPlay && cachedResults.length > 0,
      });
      if (autoPlay && cachedResults.length > 0) this.player.play(0);
      return;
    }

    $debug.info('AIOStreams: cache miss, querying API', { cacheKey });
    const searchStart = Date.now();
    try {
      const id = formatIdForSearch(parsedId);
      $debug.debug(
        'AIOStreams: final id, type, season, episode sent to search endpoint',
        {
          id,
          type: mediaType,
          season: parsedId.season,
          episode: parsedId.episode,
        }
      );
      const searchResponse = await api.search(
        mediaType,
        id,
        parsedId.season,
        parsedId.episode
      );
      searchMs = Date.now() - searchStart;
      const results = searchResponse.results.map(toStreamResult);
      this.cache.set(cacheKey, results);
      $debug.info('AIOStreams: search complete', {
        count: results.length,
        searchMs,
        errorCount: searchResponse.errors?.length ?? 0,
      });
      this.applyResultsToPanel({
        results,
        loading: false,
        error: null,
        episodeInfo,
        timeTakenMs: Date.now() - startTime,
        animeLookupMs,
        searchMs,
        fromCache: false,
        errors: searchResponse.errors ?? [],
        statistics: searchResponse.statistics ?? [],
        lookup,
        sessionId,
        autoPlay: autoPlay && results.length > 0,
      });
      if (autoPlay && results.length > 0) this.player.play(0);
    } catch (err) {
      searchMs = Date.now() - searchStart;
      console.error('AIOStreams: stream search failed', err);
      const msg = err instanceof Error ? err.message : String(err);
      this.applyResultsToPanel({
        results: [],
        loading: false,
        error: msg,
        episodeInfo,
        timeTakenMs: Date.now() - startTime,
        animeLookupMs,
        searchMs,
        fromCache: false,
        errors: [],
        statistics: [],
        lookup,
        sessionId,
        autoPlay: false,
      });
    }
  }

  private applyResultsToPanel(state: WebviewState): void {
    this.panel.wvState.set(state);
  }

  refreshLastQuery(): void {
    if (this.lastCacheKey) this.cache.invalidate(this.lastCacheKey);
    const anime = this.pendingAnime.get();
    const ep = this.pendingEp.get();
    if (anime && ep) void this.fetch(anime, ep);
  }

  retryLastQuery(): void {
    const anime = this.pendingAnime.get();
    const ep = this.pendingEp.get();
    if (anime && ep) void this.fetch(anime, ep);
  }
}
