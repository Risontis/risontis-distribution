/****
 * File: Bootstrap.js
 * Description: Host bootstrap for Control Center entrypoints plus DocumentProperties bridges. Enforces host-only policy.
 * Version: 2.0.0
 * Date: 2025-12-10
 */

function hostCheckForUpdatesFromMenu() {
  try {
    var HOST_METADATA_URL = 'https://raw.githubusercontent.com/risontis/risontis-distribution/main/host/current/host.json';

    function readCanonicalHostJson_() {
      var res = UrlFetchApp.fetch(HOST_METADATA_URL, { muteHttpExceptions: true });
      var code = res.getResponseCode();
      if (code !== 200) throw new Error('host.json fetch failed with status ' + code);
      var raw = res.getContentText();
      return JSON.parse(raw);
    }

    var hostJson = readCanonicalHostJson_();
    var latest = (hostJson && hostJson.latestRelease && typeof hostJson.latestRelease === 'object')
      ? hostJson.latestRelease
      : {};
    var version = latest.version != null ? String(latest.version) : 'n/a';
    var date = latest.date != null ? String(latest.date) : 'n/a';
    var summary = latest.summary != null ? String(latest.summary) : '';
    var notes = latest.notes != null ? String(latest.notes) : '';
    var updatesUrl = hostJson && hostJson.updatesUrl ? String(hostJson.updatesUrl) : 'https://risontis.com/updates';

    try { Logger.log('[UpdateCheck] Latest release ' + version + ' (' + date + ').'); } catch (_) {}

    var tpl = HtmlService.createTemplateFromFile('UpdateDialog');
    tpl.version = version;
    tpl.date = date;
    tpl.summary = summary;
    tpl.notes = notes || '';
    tpl.localVersion = (globalThis.RisontisCoreLibrary &&
      RisontisCoreLibrary.Constants &&
      RisontisCoreLibrary.Constants.SYSTEM_DEFAULTS &&
      RisontisCoreLibrary.Constants.SYSTEM_DEFAULTS.version)
      ? String(RisontisCoreLibrary.Constants.SYSTEM_DEFAULTS.version)
      : '';
    tpl.updatesUrl = updatesUrl;

    var html = tpl.evaluate()
      .setWidth(420)
      .setHeight(260);

    SpreadsheetApp.getUi().showModalDialog(html, ' ');
    return hostJson;
  } catch (_) {
    try {
      SpreadsheetApp.getUi().alert(
        'Risontis Update Check Failed',
        'The update check could not be completed.\nNo changes have been made.',
        SpreadsheetApp.getUi().ButtonSet.OK
      );
    } catch (_ui) {}
  }
  return null;
}

/**
 * Drive app-folder guard:
 * - Require RISONTIS_APP_FOLDER_ID in DocumentProperties (no fallback binding).
 * - Enforce routing config to use the bound folder id.
 */
(function initDriveAppFolderGuard_(){
  var DP_KEY = 'RISONTIS_APP_FOLDER_ID';

  function readBoundAppFolderId_() {
    var dp = PropertiesService.getDocumentProperties();
    var bound = (dp.getProperty(DP_KEY) || '').trim();
    if (!bound) throw new Error('[SECURITY] RISONTIS_APP_FOLDER_ID not bound. Run Setup.');
    return bound;
  }

  function assertConfigBound_(config) {
    var bound = readBoundAppFolderId_();
    var cfgId = config && config.RISONTIS_APP_FOLDER_ID ? String(config.RISONTIS_APP_FOLDER_ID).trim() : '';
    if (!cfgId) throw new Error('[SECURITY] RISONTIS_APP_FOLDER_ID missing in config.');
    if (cfgId !== bound) throw new Error('[SECURITY] RISONTIS_APP_FOLDER_ID mismatch with bound folder.');
    return bound;
  }

  function resolveGuardConfig_(config) {
    var cfgId = config && config.RISONTIS_APP_FOLDER_ID ? String(config.RISONTIS_APP_FOLDER_ID).trim() : '';
    if (cfgId) return config;
    var runtime = (globalThis.CURRENT_RUNTIME_CONFIG && typeof globalThis.CURRENT_RUNTIME_CONFIG === 'object')
      ? globalThis.CURRENT_RUNTIME_CONFIG
      : null;
    var runtimeId = runtime && runtime.RISONTIS_APP_FOLDER_ID ? String(runtime.RISONTIS_APP_FOLDER_ID).trim() : '';
    return runtimeId ? runtime : config;
  }

  function wrapGetOrCreateRoot_(origFn) {
    return function(config) {
      var effectiveConfig = resolveGuardConfig_(config);
      assertConfigBound_(effectiveConfig);
      return origFn(effectiveConfig);
    };
  }

  function wrapResolveFolder_(origFn) {
    return function(key, config) {
      var effectiveConfig = resolveGuardConfig_(config);
      assertConfigBound_(effectiveConfig);
      return origFn(key, effectiveConfig);
    };
  }

  try {
    if (typeof getOrCreateRoot_ === 'function') {
      getOrCreateRoot_ = wrapGetOrCreateRoot_(getOrCreateRoot_);
    }
    if (typeof resolveFolder_ === 'function') {
      resolveFolder_ = wrapResolveFolder_(resolveFolder_);
    }
  } catch (e) {
    try { Logger.log('[Bootstrap][DriveAppFolderGuard] init failed: ' + (e && e.message ? e.message : e)); } catch(_) {}
    throw e;
  }
})();

/**
 * Installable onOpen handler (authorized context) that performs dual-key auto-open.
 */
function onOpenAutoOpen_() {
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    if (!ss) return;
    var hasAssets = !!ss.getSheetByName('Assets');
    var hasSettings = !!ss.getSheetByName('Settings');
    if (hasAssets && hasSettings) {
      openLibraryControlCenter();
    }
  } catch (e) {
    try { Logger.log('[onOpenAutoOpen_/ERR] ' + (e && e.message ? e.message : e)); } catch(_) {}
  }
}


/** Compatibility stubs to keep optional library helpers graceful when absent. */

globalThis.__ensureHomeFolder_ = globalThis.__ensureHomeFolder_ || function(){ /* no-op */ };
globalThis.__moveFileToFolderAndRename_ = globalThis.__moveFileToFolderAndRename_ || function(){ /* no-op */ };

(function initSettingsPersistenceBridge_(){
  try {
    globalThis.RisontisHostPersistence = globalThis.RisontisHostPersistence || {};
    if (!globalThis.RisontisHostPersistence.Settings) {
      var dp = PropertiesService.getDocumentProperties();
      globalThis.RisontisHostPersistence.Settings = {
        read: function(key){ return dp.getProperty(key); },
        write: function(key, value){
          if (value === null || typeof value === 'undefined') {
            dp.deleteProperty(key);
            return;
          }
          dp.setProperty(key, String(value));
        },
        delete: function(key){ dp.deleteProperty(key); },
        readAll: function(){
          try {
            var props = dp.getProperties();
            return props ? Object.assign({}, props) : {};
          } catch (_) {
            return {};
          }
        }
      };
      publishHostPersistenceBridge_();
    }
  } catch (e) {
    try { Logger.log('[Bootstrap] Settings persistence bridge init failed: ' + (e && e.message ? e.message : e)); } catch (_) {}
  }
})();

(function initMonitorPersistenceBridge_(){
  try {
    globalThis.RisontisHostPersistence = globalThis.RisontisHostPersistence || {};
    if (!globalThis.RisontisHostPersistence.Monitor) {
      var dp = PropertiesService.getDocumentProperties();
      function readAllMonitorProps_() {
        var props = dp.getProperties();
        if (!props || typeof props !== 'object') {
          throw new Error('[Bootstrap] Monitor persistence bridge: DocumentProperties returned empty map');
        }
        return Object.assign({}, props);
      }
      globalThis.RisontisHostPersistence.Monitor = {
        readAll: function () {
          return readAllMonitorProps_();
        },
        buildDto: function () {
          var props = readAllMonitorProps_();
          var buildFn = (globalThis.RisontisCoreLibrary && typeof RisontisCoreLibrary.buildMonitorDto === 'function')
            ? RisontisCoreLibrary.buildMonitorDto
            : null;
          if (!buildFn) {
            throw new Error('[Bootstrap] Monitor persistence bridge: buildMonitorDto unavailable on library alias');
          }
          var dto = buildFn(props);
          if (!dto || dto.ok === false) {
            var reason = (dto && dto.reason) ? dto.reason : 'unknown';
            throw new Error('[Bootstrap] Monitor persistence bridge: buildMonitorDto failed: ' + reason);
          }
          return dto;
        }
      };
      publishHostPersistenceBridge_();
    }
  } catch (e) {
    try { Logger.log('[Bootstrap] Monitor persistence bridge init failed: ' + (e && e.message ? e.message : e)); } catch (_) {}
  }
})();

/** Trades archive persistence bridge (DocumentProperties → DTO). */
var TRADES_MANIFEST_REGEX = /^ARCHIVE_CACHE_TRADES_(\d{4})_MANIFEST_V1$/;
var __tradesArchiveDtoCache = null;
function buildTradesArchiveDtoFromProps_(allProps) {
  var props = allProps || {};
  var manifestKeys = Object.keys(props).filter(function(k){ return TRADES_MANIFEST_REGEX.test(k); }).sort();
  if (!manifestKeys.length) {
    try {
      Logger.log('[Bootstrap] Trades archive manifest missing. Returning empty archive DTO.');
    } catch (_) {}
    return {
      version: 1,
      generatedAt: new Date().toISOString(),
      timezone: (function(){
        try {
          var ss = SpreadsheetApp.getActiveSpreadsheet();
          var tz = ss ? ss.getSpreadsheetTimeZone() : null;
          if (tz) return tz;
        } catch (_) {}
        try { return Session.getScriptTimeZone(); } catch (_) {}
        return 'Etc/UTC';
      })(),
      props: {},
      years: [],
      stats: { archiveRowCount: 0 }
    };
  }
  var filtered = {};
  var years = [];
  var archiveRowCount = 0;

  manifestKeys.forEach(function(manifestKey){
    var manifestRaw = props[manifestKey];
    if (!manifestRaw) throw new Error('[Bootstrap] Trades archive manifest payload missing: ' + manifestKey);
    var manifest;
    try { manifest = JSON.parse(manifestRaw); } catch (e) { throw new Error('[Bootstrap] Trades archive manifest JSON invalid: ' + manifestKey); }
    if (!manifest || !Array.isArray(manifest.parts) || !manifest.parts.length) {
      try { Logger.log('[Bootstrap] Skipping manifest with no parts: ' + manifestKey); } catch(_){}
      return;
    }
    filtered[manifestKey] = manifestRaw;
    var yearMatch = manifestKey.match(TRADES_MANIFEST_REGEX);
    var yearMeta = { year: yearMatch ? Number(yearMatch[1]) : null, manifestKey: manifestKey, partKeys: [], rowCount: 0 };

    manifest.parts.forEach(function(partKey){
      var chunkRaw = props[partKey];
      if (!chunkRaw) { try { Logger.log('[Bootstrap] Skipping missing chunk: ' + partKey); } catch(_){}
        return; }
      filtered[partKey] = chunkRaw;
      var chunk;
      try { chunk = JSON.parse(chunkRaw); } catch (e) { try { Logger.log('[Bootstrap] Skipping invalid chunk JSON: ' + partKey); } catch(_){}
        return; }
      var trades = Array.isArray(chunk.trades) ? chunk.trades : (Array.isArray(chunk.rows) ? chunk.rows : null);
      if (!Array.isArray(trades) || !trades.length) { try { Logger.log('[Bootstrap] Skipping empty/invalid chunk: ' + partKey); } catch(_){}
        return; }
      archiveRowCount += trades.length;
      yearMeta.rowCount += trades.length;
      yearMeta.partKeys.push(partKey);
    });
    if (yearMeta.partKeys.length) {
      years.push(yearMeta);
    }
  });

  years.sort(function(a,b){
    if (a.year === b.year) return a.manifestKey.localeCompare(b.manifestKey);
    return (a.year || 0) - (b.year || 0);
  });

  var tz = null;
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    tz = ss ? ss.getSpreadsheetTimeZone() : null;
  } catch (_) { tz = null; }
  if (!tz) {
    try { tz = Session.getScriptTimeZone(); } catch (_) { tz = 'Etc/UTC'; }
  }

  return {
    version: 1,
    generatedAt: new Date().toISOString(),
    timezone: tz || 'Etc/UTC',
    props: filtered,
    years: years,
    stats: { archiveRowCount: archiveRowCount }
  };
}

function hostGetTradesArchiveDto_() {
  if (__tradesArchiveDtoCache) return __tradesArchiveDtoCache;
  var dp = PropertiesService.getDocumentProperties();
  var props = dp.getProperties();
  __tradesArchiveDtoCache = buildTradesArchiveDtoFromProps_(props);
  return __tradesArchiveDtoCache;
}
function invalidateTradesArchiveDtoCache_() {
  __tradesArchiveDtoCache = null;
}
globalThis.hostGetTradesArchiveDto_ = hostGetTradesArchiveDto_;
globalThis.invalidateTradesArchiveDtoCache_ = invalidateTradesArchiveDtoCache_;

(function initTradesArchivePersistenceBridge_(){
  try {
    globalThis.RisontisHostPersistence = globalThis.RisontisHostPersistence || {};
    if (!globalThis.RisontisHostPersistence.Trades) {
      globalThis.RisontisHostPersistence.Trades = {
        getArchiveDto: hostGetTradesArchiveDto_,
        buildDto: hostGetTradesArchiveDto_,
        invalidate: invalidateTradesArchiveDtoCache_
      };
      publishHostPersistenceBridge_();
    }
  } catch (e) {
    try { Logger.log('[Bootstrap] Trades archive bridge init failed: ' + (e && e.message ? e.message : e)); } catch (_) {}
  }
})();

/** Equity NAV persistence bridge (DocumentProperties → DTO). */
var EQUITY_BASE_REGEX = /^ARCHIVE_CACHE_EQUITY_(\d{4})(?:$|_)/;
var __equityNavDtoCache = null;
var __stylingStateDtoCache = null;
var __settingsCacheDtoCache = null;

function readEquityChunkedObject_(props, base) {
  try {
    var mfKey = base + '_MANIFEST_V1';
    var mfRaw = props[mfKey];
    if (!mfRaw) return null;
    var mf = JSON.parse(mfRaw);
    var parts = Number(mf.parts || 0);
    if (!Number.isFinite(parts) || parts <= 0) return null;
    var buf = '';
    for (var i = 1; i <= parts; i++) {
      var pKey = base + '_P' + i + '_V1';
      var chunk = props[pKey];
      if (typeof chunk !== 'string') return null;
      buf += chunk;
    }
    return JSON.parse(buf);
  } catch (_) {
    return null;
  }
}

function readEquityYearFromProps_(props, yearStr) {
  var base = 'ARCHIVE_CACHE_EQUITY_' + yearStr;
  if (!props) return null;

  // Prefer flat key
  var flatRaw = props[base];
  if (typeof flatRaw === 'string') {
    try {
      var flat = JSON.parse(flatRaw);
      if (flat && Array.isArray(flat.data)) return flat;
    } catch (_) {}
  }

  // Fallback: chunked manifest/parts
  var chunked = readEquityChunkedObject_(props, base);
  if (chunked && Array.isArray(chunked.data)) return chunked;
  return null;
}

function buildEquityNavDtoFromProps_(allProps) {
  var props = allProps || {};
  var yearsSet = Object.create(null);
  Object.keys(props).forEach(function(k){
    var m = k.match(EQUITY_BASE_REGEX);
    if (m && m[1]) yearsSet[m[1]] = true;
  });
  var years = Object.keys(yearsSet).sort();
  if (!years.length) throw new Error('[Bootstrap] Equity NAV cache missing (ARCHIVE_CACHE_EQUITY_<YEAR>)');

  var rows = [];
  years.forEach(function(y){
    var payload = readEquityYearFromProps_(props, y);
    if (payload && Array.isArray(payload.data) && payload.data.length) {
      payload.data.forEach(function(r){
        var d = (r && r.date != null) ? String(r.date).slice(0,10) : '';
        if (!d) return;
        rows.push({
          dateISO: d,
          equity: Number(r.equity) || 0,
          dailyPnl: Number(r.dailyPnl) || 0
        });
      });
    }
  });

  if (!rows.length) throw new Error('[Bootstrap] Equity NAV rows missing or empty.');

  rows.sort(function(a,b){ return String(a.dateISO).localeCompare(String(b.dateISO)); });

  var tz = null;
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    tz = ss ? ss.getSpreadsheetTimeZone() : null;
  } catch (_) { tz = null; }
  if (!tz) {
    try { tz = Session.getScriptTimeZone(); } catch (_) { tz = 'Etc/UTC'; }
  }

  var summary = {
    first: rows[0].dateISO || null,
    last: rows[rows.length - 1].dateISO || null,
    count: rows.length
  };

  return {
    version: 1,
    generatedAt: new Date().toISOString(),
    timezone: tz || 'Etc/UTC',
    rows: rows,
    summary: summary
  };
}

function hostGetEquityNavDto_() {
  if (__equityNavDtoCache) return __equityNavDtoCache;
  var dp = PropertiesService.getDocumentProperties();
  var props = dp.getProperties();
  __equityNavDtoCache = buildEquityNavDtoFromProps_(props);
  return __equityNavDtoCache;
}

function invalidateEquityNavDtoCache_() {
  __equityNavDtoCache = null;
}

globalThis.hostGetEquityNavDto_ = hostGetEquityNavDto_;
globalThis.invalidateEquityNavDtoCache_ = invalidateEquityNavDtoCache_;

(function initEquityNavPersistenceBridge_(){
  try {
    globalThis.RisontisHostPersistence = globalThis.RisontisHostPersistence || {};
    if (!globalThis.RisontisHostPersistence.Equity) {
      globalThis.RisontisHostPersistence.Equity = {
        getNavDto: hostGetEquityNavDto_,
        buildDto: hostGetEquityNavDto_,
        invalidate: invalidateEquityNavDtoCache_
      };
      publishHostPersistenceBridge_();
    }
  } catch (e) {
    try { Logger.log('[Bootstrap] Equity NAV bridge init failed: ' + (e && e.message ? e.message : e)); } catch (_) {}
  }
})();

/** Styling state persistence bridge (DocumentProperties → DTO with host counters). */
var STYLING_COUNTER_REGEX = /^styleCount_(.+)$/;
function buildStylingStateDtoFromProps_(props) {
  var p = props || {};
  var styleCount = {};
  Object.keys(p).forEach(function(k){
    var m = k.match(STYLING_COUNTER_REGEX);
    if (m && m[1]) {
      var sheet = m[1];
      styleCount[sheet] = Number(p[k]) || 0;
    }
  });
  var runs = Number(p.STYLING_RUNS_V1) || 0;
  var lastTs = (p.LAST_STYLE_TS || '').trim() || null;
  var failures = Number(p.STYLING_FAILURE_COUNT) || 0;
  return {
    version: 1,
    updatedAt: new Date().toISOString(),
    counters: {
      styleCount: styleCount,
      runs: runs,
      lastStyleTs: lastTs || null
    },
    failures: failures
  };
}
function hostGetStylingStateDto_() {
  if (__stylingStateDtoCache) return __stylingStateDtoCache;
  var dp = PropertiesService.getDocumentProperties();
  var props = dp.getProperties() || {};
  __stylingStateDtoCache = buildStylingStateDtoFromProps_(props);
  return __stylingStateDtoCache;
}
function invalidateStylingStateDtoCache_() {
  __stylingStateDtoCache = null;
}
function applyStylingStateUpdates_(updates) {
  if (!updates || typeof updates !== 'object') return { ok:false, reason:'no updates' };
  var dp = PropertiesService.getDocumentProperties();
  var props = dp.getProperties() || {};
  var styleCount = {};
  Object.keys(props).forEach(function(k){
    var m = k.match(STYLING_COUNTER_REGEX);
    if (m && m[1]) {
      styleCount[m[1]] = Number(props[k]) || 0;
    }
  });
  var increments = updates.increments || {};
  Object.keys(increments).forEach(function(sheet){
    var cur = styleCount[sheet] || 0;
    var next = cur + Number(increments[sheet] || 0);
    styleCount[sheet] = next;
    dp.setProperty('styleCount_' + sheet, String(next));
  });
  var resets = Array.isArray(updates.resets) ? updates.resets : [];
  resets.forEach(function(sheet){
    styleCount[sheet] = 0;
    dp.setProperty('styleCount_' + sheet, '0');
  });
  if (typeof updates.runsIncrement === 'number' && isFinite(updates.runsIncrement)) {
    var runsCur = Number(props.STYLING_RUNS_V1) || 0;
    dp.setProperty('STYLING_RUNS_V1', String(runsCur + Math.max(0, Math.floor(updates.runsIncrement))));
  }
  if (updates.lastStyleTs) {
    dp.setProperty('LAST_STYLE_TS', String(updates.lastStyleTs));
  }
  invalidateStylingStateDtoCache_();
  return { ok:true, styleCount: styleCount };
}
globalThis.hostGetStylingStateDto_ = hostGetStylingStateDto_;
globalThis.applyStylingStateUpdates_ = applyStylingStateUpdates_;
globalThis.invalidateStylingStateDtoCache_ = invalidateStylingStateDtoCache_;
(function initStylingStatePersistenceBridge_(){
  try {
    globalThis.RisontisHostPersistence = globalThis.RisontisHostPersistence || {};
    if (!globalThis.RisontisHostPersistence.StylingState) {
      globalThis.RisontisHostPersistence.StylingState = {
        getDto: hostGetStylingStateDto_,
        applyUpdates: applyStylingStateUpdates_,
        invalidate: invalidateStylingStateDtoCache_
      };
      publishHostPersistenceBridge_();
    }
  } catch (e) {
    try { Logger.log('[Bootstrap] StylingState bridge init failed: ' + (e && e.message ? e.message : e)); } catch (_) {}
  }
})();

/** Settings cache persistence bridge (SETTINGS_CACHE_V1 + SETTINGS_HASH_V1). */
function buildSettingsCacheDtoFromProps_(props){
  var p = props || {};
  if (!p.SETTINGS_CACHE_V1 || !p.SETTINGS_HASH_V1) return null;
  try {
    var snapshot = JSON.parse(p.SETTINGS_CACHE_V1);
    return {
      version: 1,
      hash: String(p.SETTINGS_HASH_V1 || ''),
      snapshot: snapshot,
      updatedAt: new Date().toISOString()
    };
  } catch (_) {
    return null;
  }
}
function hostGetSettingsCacheDto_(){
  if (__settingsCacheDtoCache) return __settingsCacheDtoCache;
  var dp = PropertiesService.getDocumentProperties();
  var props = dp.getProperties() || {};
  __settingsCacheDtoCache = buildSettingsCacheDtoFromProps_(props);
  return __settingsCacheDtoCache;
}
function invalidateSettingsCacheDto_(){
  __settingsCacheDtoCache = null;
}
function applySettingsCacheUpdate_(update){
  if (!update || typeof update !== 'object') return { ok:false, reason:'no update' };
  var hash = update.hash || '';
  var snapshot = update.snapshot || {};
  var dp = PropertiesService.getDocumentProperties();
  try {
    dp.setProperty('SETTINGS_CACHE_V1', JSON.stringify(snapshot));
    dp.setProperty('SETTINGS_HASH_V1', String(hash));
    invalidateSettingsCacheDto_();
    return { ok:true };
  } catch (e) {
    return { ok:false, reason:(e && e.message) ? e.message : String(e||'unknown') };
  }
}
globalThis.hostGetSettingsCacheDto_ = hostGetSettingsCacheDto_;
globalThis.applySettingsCacheUpdate_ = applySettingsCacheUpdate_;
globalThis.invalidateSettingsCacheDto_ = invalidateSettingsCacheDto_;
(function initSettingsCacheBridge_(){
  try {
    globalThis.RisontisHostPersistence = globalThis.RisontisHostPersistence || {};
    if (!globalThis.RisontisHostPersistence.SettingsCache) {
      globalThis.RisontisHostPersistence.SettingsCache = {
        getDto: hostGetSettingsCacheDto_,
        applyUpdates: applySettingsCacheUpdate_,
        invalidate: invalidateSettingsCacheDto_
      };
      publishHostPersistenceBridge_();
    }
  } catch (e) {
    try { Logger.log('[Bootstrap] SettingsCache bridge init failed: ' + (e && e.message ? e.message : e)); } catch (_) {}
  }
})();

function publishHostPersistenceBridge_() {
  try {
    if (!globalThis.RisontisHostPersistence) return;
    var state = globalThis.__HOST_BRIDGE_PUBLISH_STATE__;
    if (!state || typeof state !== 'object') {
      state = globalThis.__HOST_BRIDGE_PUBLISH_STATE__ = {
        lastPublishedSignature: null,
        missingTargetLogged: false
      };
    }
    var signature = (function(bridge){
      try {
        return Object.keys(bridge || {}).sort().map(function(name){
          var node = bridge[name];
          if (!node || typeof node !== 'object') return name + ':<nonobj>';
          var methods = Object.keys(node)
            .filter(function(k){ return typeof node[k] === 'function'; })
            .sort()
            .join(',');
          return name + ':' + methods;
        }).join('|');
      } catch (_) {
        return 'sig-error';
      }
    })(globalThis.RisontisHostPersistence);
    var lib = (typeof globalThis.RisontisCoreLibrary !== 'undefined') ? globalThis.RisontisCoreLibrary : null;
    if (lib && typeof lib.setHostPersistenceBridge === 'function') {
      lib.setHostPersistenceBridge(globalThis.RisontisHostPersistence);
      if (state.lastPublishedSignature !== signature) {
        state.lastPublishedSignature = signature;
        state.missingTargetLogged = false;
        try { Logger.log('[Bootstrap] Published host persistence bridge to library (signature=' + signature + ').'); } catch (_) {}
      }
    } else {
      if (!state.missingTargetLogged) {
        state.missingTargetLogged = true;
        try { Logger.log('[Bootstrap] Host persistence bridge not published (library alias missing or setHostPersistenceBridge unavailable).'); } catch (_) {}
      }
    }
  } catch (err) {
    try { Logger.log('[Bootstrap] publishHostPersistenceBridge_ failed: ' + (err && err.message ? err.message : err)); } catch (_) {}
  }
}

function normalizeIsoDay_(d) {
  if (d instanceof Date && !isNaN(d)) return d.toISOString().slice(0, 10);
  if (typeof d === 'string' && d.length >= 10) return d.slice(0, 10);
  return null;
}

function filterEquityRowsForQuery_(rows, query, tz) {
  var arr = Array.isArray(rows) ? rows.slice() : [];
  arr.sort(function(a,b){ return String(a.dateISO).localeCompare(String(b.dateISO)); });
  if (!query) return arr;

  var from = normalizeIsoDay_(query.from || query.startDate);
  var to = normalizeIsoDay_(query.to || query.endDate);

  var days = Number(query && query.days);
  if (!from && Number.isFinite(days) && days > 0) {
    var DAY_MS = 24 * 60 * 60 * 1000;
    var start = new Date(Date.now() - (Math.floor(days) * DAY_MS));
    from = start.toISOString().slice(0, 10);
  }

  if (from) arr = arr.filter(function(r){ return String(r.dateISO) >= from; });
  if (to) arr = arr.filter(function(r){ return String(r.dateISO) <= to; });
  return arr;
}

function buildEquityNavResponse_(dto, query) {
  var baseDto = dto || {};
  var filtered = filterEquityRowsForQuery_(baseDto.rows, query, baseDto.timezone);
  var summary = filtered.length
    ? { first: filtered[0].dateISO, last: filtered[filtered.length - 1].dateISO, count: filtered.length }
    : { first: null, last: null, count: 0 };

  var navSeries = filtered.map(function(r){ return { t: r.dateISO, v: Number(r.equity) || 0 }; });
  var dailySeries = filtered.map(function(r){ return { t: r.dateISO, v: Number(r.dailyPnl) || 0 }; });

  var dtoOut = Object.assign({}, baseDto, { rows: filtered, summary: summary });
  return {
    ok: true,
    dto: dtoOut,
    nav: navSeries,
    daily: dailySeries,
    timezone: dtoOut.timezone || null
  };
}

function hostGetEquityNavData_v2(query) {
  try {
    var dto = hostGetEquityNavDto_();
    return buildEquityNavResponse_(dto, query || {});
  } catch (e) {
    try { Logger.log('[Bootstrap#hostGetEquityNavData_v2 ERR] ' + (e && e.message ? e.message : e)); } catch(_) {}
    return { ok: false, nav: [], daily: [], error: (e && e.message ? e.message : String(e || 'unknown')) };
  }
}

function hostGetEquityNavDto() {
  return hostGetEquityNavDto_();
}

globalThis.hostGetEquityNavData_v2 = hostGetEquityNavData_v2;
globalThis.hostGetEquityNavDto = hostGetEquityNavDto;

/**
 * Copies runtime credentials from UserProperties into cfg.RuntimeCredentials.
 * UserProperties remain host-only; the library never reads them directly.
 */
function injectRuntimeCredsFromUP_(cfg) {
  try {
    cfg = cfg || (typeof GLOBAL_CONFIG !== 'undefined' ? GLOBAL_CONFIG : {});
    var up = PropertiesService.getUserProperties();
    var k = (up.getProperty('BITVAVO_API_KEY') || '').trim();
    var s = (up.getProperty('BITVAVO_API_SECRET') || '').trim();
    var m = (up.getProperty('ALERT_EMAIL')       || '').trim();
    cfg.RuntimeCredentials = cfg.RuntimeCredentials || {};
    if (k) cfg.RuntimeCredentials.apiKey = k;
    if (s) cfg.RuntimeCredentials.apiSecret = s;
    if (m) cfg.RuntimeCredentials.notificationEmail = m;
    return cfg;
  } catch (e) {
    Logger.log('[Bootstrap] injectRuntimeCredsFromUP_ failed: ' + (e && e.message ? e.message : e));
    return cfg;
  }
}

function requireGlobalConfig_() {
  if (typeof GLOBAL_CONFIG === 'undefined' || !GLOBAL_CONFIG) {
    throw new Error('GLOBAL_CONFIG is undefined in host. Provide TradingConfig.gs with a valid GLOBAL_CONFIG.');
  }
}

/** Guard: ensure the library alias exists. */
function core_() {
  if (typeof RisontisCoreLibrary === 'undefined' || !RisontisCoreLibrary) {
    throw new Error('Library alias "RisontisCoreLibrary" not found. Add the library in Resources → Libraries…');
  }
  publishHostPersistenceBridge_();
  return RisontisCoreLibrary;
}

/** Minimal host-side backfill to avoid missing Logging in standalone triggers.
 * Uses Constants.SYSTEM_DEFAULTS when available; no hard host defaults beyond that.
 */
function ensureLoggingDefaults_(cfg) {
  cfg = cfg || {};
  cfg.Logging = cfg.Logging || {};
  if (!cfg.Logging.dateTimeFormat) {
    try {
      var df = (globalThis.Constants && globalThis.Constants.SYSTEM_DEFAULTS && globalThis.Constants.SYSTEM_DEFAULTS.dateTimeFormat)
        ? globalThis.Constants.SYSTEM_DEFAULTS.dateTimeFormat
        : 'dd/MM/yyyy HH:mm:ss';
      cfg.Logging.dateTimeFormat = df;
    } catch (_e) {
      cfg.Logging.dateTimeFormat = 'dd/MM/yyyy HH:mm:ss';
    }
  }
  if (!cfg.Logging.timeZone) {
    try {
      var tz = (SpreadsheetApp.getActive() && SpreadsheetApp.getActive().getSpreadsheetTimeZone())
        || Session.getScriptTimeZone()
        || 'Etc/UTC';
      cfg.Logging.timeZone = tz;
    } catch (_e2) {
      cfg.Logging.timeZone = Session.getScriptTimeZone() || 'Etc/UTC';
    }
  }
  return cfg;
}

// Execution-level guard: capture fatal trading run failures and surface them to the user
function logExecutionFailure_(err) {
  try {
    var dp = PropertiesService.getDocumentProperties();
    var now = new Date();
    var nowMs = now.getTime();
    var msg = (err && err.message) ? String(err.message) : String(err || 'unknown');
    var tsIso = now.toISOString();
    var keyMsg = 'LAST_TRADE_RUN_ERROR';
    var keyTs = 'LAST_TRADE_RUN_ERROR_TS';

    // Helper: decide if this error is mail-worthy (explicit allowlist)
    function isMailWorthy_(m) {
      if (!m) return false;
      if (m.indexOf('Trial license allows max') >= 0) return true;
      if (m.indexOf('License missing: please start a trial or activate a subscription') >= 0) return true;
      if (m.indexOf('License expired') >= 0) return true;
      if (m.indexOf('License enforcement') >= 0 || m.indexOf('Enforcement failed') >= 0) return true;
      if (m === '[SECURITY] Trading=true but secrets missing') return true;
      return false;
    }

    var prevMsg = dp.getProperty(keyMsg) || '';
    var prevTs = Number(dp.getProperty(keyTs) || 0);
    var throttled = (prevMsg === msg); // message-based: only new messages mail

    // Persist minimal DP flags for optional UI surfaces
    try {
      dp.setProperty(keyMsg, msg);
      dp.setProperty(keyTs, String(nowMs));
    } catch (_) {}

    // Write a minimal SystemLog entry (header-only schema: Timestamp, Level, Message)
    try {
      var ss = SpreadsheetApp.getActiveSpreadsheet();
      var sh = ss && ss.getSheetByName('SystemLog');
      if (sh) {
        sh.insertRowBefore(2);
        sh.getRange(2, 1, 1, 3).setValues([[now, 'FATAL', msg]]);
      }
    } catch (_) {}

    // Email notification (uses effective user email). Message-based throttle (new incidents only).
    try {
      var email = '';
      try {
        if (typeof getEffectiveUserEmail === 'function') {
          email = getEffectiveUserEmail();
        } else if (typeof getEffectiveUserEmail_ === 'function') {
          email = getEffectiveUserEmail_();
        }
      } catch (_) { email = ''; }
      email = (email && typeof email === 'string') ? email.trim() : '';
      var subject = 'Risontis – Trading run failed';
      var body = [
        'A trading run failed at ' + tsIso + '.',
        '',
        'Error: ' + msg,
        '',
        'Trading will remain paused until this issue is resolved.'
      ].join('\n');

      if (!isMailWorthy_(msg)) {
        // not a user-actionable category: never mail
      } else if (!email) {
        // no email resolved; skip
      } else if (throttled) {
        // same error already mailed; skip
      } else {
        try {
          GmailApp.sendEmail(email, subject, body);
        } catch (eMail) {
          throw eMail;
        }
      }
    } catch (_) {}
  } catch (_) {}
}

/**
 * Early hydrate of PersistedState and TradingEnabled mirror from DocumentProperties.
 * Used before SetupRunner.ensureInstall so it sees correct flags.
 */
function hydratePersistedStateEarly_(cfg) {
  try {
    cfg = cfg || {};
    cfg.PersistedState = cfg.PersistedState || {};
    var dp = PropertiesService.getDocumentProperties();
    var all = dp.getProperties() || {};
    // Credentials verified flag
    if (typeof all['CREDENTIALS_VERIFIED'] !== 'undefined') {
      cfg.PersistedState.credentialsVerified = (String(all['CREDENTIALS_VERIFIED']).trim() === '1');
    }
    // Optional maintenance flag
    if (typeof all['MaintenanceOngoing'] !== 'undefined') {
      cfg.PersistedState.maintenanceOngoing = (String(all['MaintenanceOngoing']).trim().toLowerCase() === 'true');
    }
    // Exchange probe failure streak (for resilient outage detection)
    if (typeof all['EXCHANGE_PROBE_FAIL_COUNT'] !== 'undefined') {
      var cnt = parseInt(String(all['EXCHANGE_PROBE_FAIL_COUNT']).trim(), 10);
      if (!isNaN(cnt)) {
        cfg.PersistedState.exchangeProbeFailCount = cnt;
      }
    }
    if (typeof all['EXCHANGE_PROBE_FAIL_TS'] !== 'undefined') {
      var ts = parseInt(String(all['EXCHANGE_PROBE_FAIL_TS']).trim(), 10);
      if (!isNaN(ts)) {
        cfg.PersistedState.exchangeProbeFailTs = ts;
      }
    }
    // Trading Enabled mirror for early consumers
    try {
      var te = all['TRADING_ENABLED'];
      if (typeof te !== 'undefined') {
        var s = String(te).trim().toLowerCase();
        cfg.Settings = cfg.Settings || {};
        cfg.Settings.TRADING_ENABLED = (s === 'true' || s === 'on' || s === '1' || s === 'yes');
      }
    } catch (_te) {}
  } catch (_) {}
  return cfg;
}

/**
 * Returns canonical Assets sheet name from SSOT.
 * Preference order: library namespace (RisontisCoreLibrary.Constants.SHEETS.ASSETS) → global Constants.SHEETS.ASSETS.
 * No TradingConfig reads. Fails fast if SSOT is missing from both.
 */
function getAssetsSheetName_() {
  if (globalThis.RisontisCoreLibrary
      && RisontisCoreLibrary.Constants
      && RisontisCoreLibrary.Constants.SHEETS
      && RisontisCoreLibrary.Constants.SHEETS.ASSETS) {
    return RisontisCoreLibrary.Constants.SHEETS.ASSETS; // 'Assets'
  }
  if (globalThis.Constants && Constants.SHEETS && Constants.SHEETS.ASSETS) {
    return Constants.SHEETS.ASSETS;
  }
  throw new Error('[BOOTSTRAP] Missing SSOT for sheet names: Constants.SHEETS.ASSETS not found (library nor global).');
}


/**
 * Verifies that the canonical Assets sheet exists and has data rows.
 * Host validates only; provisioning remains inside the library.
 */
function assertAssetsSheetReady_() {
  var assetsName = getAssetsSheetName_();
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var cfg = (typeof GLOBAL_CONFIG !== 'undefined' ? GLOBAL_CONFIG : null);

  // Runtime provisioning fully disabled; only minimal SheetManager assertion is allowed.
  if (globalThis.RisontisCoreLibrary &&
      RisontisCoreLibrary.SheetManager &&
      typeof RisontisCoreLibrary.SheetManager.ensureSheetExists === 'function') {
    RisontisCoreLibrary.SheetManager.ensureSheetExists(assetsName, 'Assets', null, cfg);
    Logger.log('[Bootstrap] ensureInstall disabled — using ensureSheetExists (runtime-safe).');
  }
  if (globalThis.RisontisCoreLibrary &&
      RisontisCoreLibrary.SheetManager &&
      typeof RisontisCoreLibrary.SheetManager.ensureSettingsSheetExists === 'function') {
    var runtimeCfg = cfg ? Object.assign({}, cfg, { Runtime: true }) : { Runtime: true };
    RisontisCoreLibrary.SheetManager.ensureSettingsSheetExists(runtimeCfg);
    Logger.log('[Bootstrap] ensureSettingsSheetExists (runtime-safe).');
  }

  var sh = ss.getSheetByName(assetsName);
  if (!sh) {
    throw new Error("[FTR] Assets sheet is missing. Run SetupRunner.ensureInstall(config) or SheetManager.ensureSheetExists('" + assetsName + "','Assets').");
  }
  if (sh.getLastRow() < 2) {
    throw new Error("[FTR] Assets sheet is header-only (no rows). Run SetupRunner.ensureInstall(config) to seed Assets.");
  }
}

// Minimal logging helpers

function logInfo(msg) {
  if (globalThis.NotificationUtils && typeof globalThis.NotificationUtils.logInfo === 'function') {
    globalThis.NotificationUtils.logInfo(msg);
  } else {
    Logger.log('[INFO] ' + msg);
  }
}

function logDebug(msg) {
  if (globalThis.NotificationUtils && typeof globalThis.NotificationUtils.logDebug === 'function') {
    globalThis.NotificationUtils.logDebug(msg);
  } else {
    Logger.log('[DEBUG] ' + msg);
  }
}

/**
 * Resolve library loadAssetsFromSheet function from either global or library alias.
 */
function resolveLoadAssetsFromSheet_() {
  if (globalThis.ConfigUtils && typeof globalThis.ConfigUtils.loadAssetsFromSheet === 'function') {
    return globalThis.ConfigUtils.loadAssetsFromSheet;
  }
  try {
    var lib = core_();
    if (lib && lib.ConfigUtils && typeof lib.ConfigUtils.loadAssetsFromSheet === 'function') {
      return lib.ConfigUtils.loadAssetsFromSheet;
    }
    if (lib && typeof lib.loadAssetsFromSheet === 'function') {
      return lib.loadAssetsFromSheet;
    }
  } catch (_e) { /* core_ throws when alias missing; will rethrow below */ }
  throw new Error('[BOOTSTRAP] loadAssetsFromSheet not found on global ConfigUtils nor library alias. Ensure CoreExports exposes ConfigUtils.loadAssetsFromSheet or a top-level loadAssetsFromSheet (v2.3+).');
}

/**
 * Fallback: define assertRoutingConfig_ only if missing (legacy-safe).
 */
if (typeof assertRoutingConfig_ !== 'function') {
  function assertRoutingConfig_(config) {
    try {
      if (typeof cfgOpt_ === 'function') {
        cfgOpt_(config);
      } else {
        if (typeof GLOBAL_CONFIG === 'undefined' || !GLOBAL_CONFIG) {
          throw new Error('[Routing] GLOBAL_CONFIG not loaded.');
        }
      }
      if (typeof folderTradeArchive_ === 'function') folderTradeArchive_(config);
      if (typeof folderAuditLogs_ === 'function') folderAuditLogs_(config);
    } catch (e) {
      Logger.log('[Bootstrap] assertRoutingConfig_ fallback failed: ' + (e && e.message ? e.message : e));
      throw e;
    }
  }
}

/** Read active assets from the canonical Assets sheet (post-migration). */
function buildActiveAssets_() {
  requireGlobalConfig_();
  assertAssetsSheetReady_();
  var loadAssetsFn = resolveLoadAssetsFromSheet_();
  var assetsName = getAssetsSheetName_();
  var assets = loadAssetsFn(assetsName) || [];
  try {
    var sample = assets.slice(0, 5).map(function(a){ return a && a.Asset ? a.Asset : String(a); }).join(', ');
    Logger.log('[Bootstrap] buildActiveAssets_: sheet=\"' + assetsName + '\", activeAssets=' + assets.length + (sample ? (' [' + sample + (assets.length > 5 ? ', …' : '') + ']') : ''));
  } catch (_e) {}
  return assets;
}

/** Build the per-run runtime config by hydrating GLOBAL_CONFIG with assets. */
function buildConfigForRun_() {
  requireGlobalConfig_();
  var assets = buildActiveAssets_();
  var cfg = Object.assign({}, GLOBAL_CONFIG, { assets: assets });
  var dp = PropertiesService.getDocumentProperties();
  cfg.RISONTIS_APP_FOLDER_ID = dp.getProperty('RISONTIS_APP_FOLDER_ID');
  // FTR: guarantee Logging defaults (tz + format) for all downstream writers
  cfg = ensureLoggingDefaults_(cfg);
  return cfg;
}

/** Minimal runtime config for archive refresh (no Assets/Settings dependency). */
function buildArchiveRefreshConfig_() {
  requireGlobalConfig_();
  var cfg = Object.assign({}, GLOBAL_CONFIG);
  var dp = PropertiesService.getDocumentProperties();
  var folderId = (dp.getProperty('RISONTIS_APP_FOLDER_ID') || '').trim();
  if (!folderId) {
    throw new Error('[Bootstrap] RISONTIS_APP_FOLDER_ID not bound. Run Setup.');
  }
  cfg.RISONTIS_APP_FOLDER_ID = folderId;
  cfg = ensureLoggingDefaults_(cfg);
  var storage = null;
  if (cfg.StorageRouting && typeof cfg.StorageRouting === 'object') {
    storage = Object.assign({}, cfg.StorageRouting);
  } else if (globalThis.Constants && Constants.STORAGE_ROUTING_DEFAULTS && typeof Constants.STORAGE_ROUTING_DEFAULTS === 'object') {
    storage = Object.assign({}, Constants.STORAGE_ROUTING_DEFAULTS);
  }
  cfg.StorageRouting = storage;
  cfg.Logging = cfg.Logging || {};
  cfg.Logging.timeZone = 'Etc/UTC';
  return cfg;
}

/** Utility: attempt to call the first existing function name on the library, forwarding args. */
function callCoreFn_(names) {
  const lib = core_();
  var args = Array.prototype.slice.call(arguments, 1);
  for (var i = 0; i < names.length; i++) {
    var n = names[i];
    if (typeof lib[n] === 'function') {
      return lib[n].apply(null, args);
    }
  }
  throw new Error('None of the expected core functions are exported: [' + names.join(', ') + ']. Check CoreExports mapping and library publish/version.');
}

/**
 * Script Properties are disallowed; any presence is treated as a security fault.
 */
globalThis.assertNoScriptProperties_ = function assertNoScriptProperties_() {
  const sp = PropertiesService.getScriptProperties();
  const keys = (sp.getKeys && sp.getKeys()) || [];
  if (keys.length > 0) {
    throw new Error('[SECURITY] Script Properties are disallowed. Found keys: ' + keys.join(', ')
      + '. Move all secrets/binding to UserProperties and purge Script Properties.');
  }
};


/**
 * UTF-8 byte length utility (no Utilities.Blob dependency).
 */
function __utf8Bytes__(s){
  s = String(s == null ? '' : s);
  var n=0; for (var i=0;i<s.length;i++){
    var c=s.charCodeAt(i);
    if (c<=0x7F) n++;
    else if (c<=0x7FF) n+=2;
    else if (c>=0xD800 && c<=0xDBFF){ i++; n+=4; }
    else n+=3;
  }
  return n;
}

/**
 * Guarded DP set: skips values that exceed configured byte limit.
 * Reads limit from GLOBAL_CONFIG.Render.DP.MAX_BYTES, default 8500.
 * Returns {ok:boolean, bytes:number, key:string}.
 */

function __dpSetJsonGuarded__(dp, key, value){
  var limit = 8500;
  try {
    if (globalThis.GLOBAL_CONFIG && GLOBAL_CONFIG.Render && GLOBAL_CONFIG.Render.DP && Number(GLOBAL_CONFIG.Render.DP.MAX_BYTES)) {
      limit = Number(GLOBAL_CONFIG.Render.DP.MAX_BYTES);
    }
  } catch(_) {}
  var s = (typeof value === 'string') ? value : JSON.stringify(value);
  var b = __utf8Bytes__(s);
  if (b <= limit) {
    dp.setProperty(key, s);
    try { Logger.log('[DP][OK] '+key+' bytes='+b+' limit='+limit); } catch(_){}
    return { ok:true, bytes:b, key:key };
  }
  try { Logger.log('[DP][SKIP] '+key+' bytes='+b+' > limit='+limit+' — falling back to chunked write.'); } catch(_){}
  // Fallback: attempt chunked write for large payloads
  try {
    if (typeof __dpSetFullChunked__ === 'function') {
      var fallbackResult = __dpSetFullChunked__(dp, key, value);
      try { Logger.log('[DP][FALLBACK] __dpSetFullChunked__ result: ' + JSON.stringify(fallbackResult)); } catch(_) {}
      return Object.assign({ok:false, bytes:b, key:key, fallback:true}, fallbackResult);
    } else {
      try { Logger.log('[DP][FALLBACK][ERR] __dpSetFullChunked__ is not defined.'); } catch(_) {}
    }
  } catch (e) {
    try { Logger.log('[DP][FALLBACK][ERR] Chunked write failed for '+key+': ' + (e && e.message ? e.message : e)); } catch(_) {}
    // Continue to return original result
  }
  return { ok:false, bytes:b, key:key };
}

/**
 * Batch-write DocumentProperties for a map of key/values.
 * Used to optimize I/O when persisting many properties at once.
 */
function __dpBatchWrite__(dp, mapObj) {
  // Batch write optimization: use setProperties for multiple keys in one call.
  if (mapObj && typeof mapObj === 'object') {
    dp.setProperties(mapObj, false);
  }
}


function __mkMonitorPersistError__(code, message, cause) {
  var e = new Error((code || 'MONITOR_V2_UNKNOWN') + ': ' + (message || 'monitor persist failure'));
  e.code = code || 'MONITOR_V2_UNKNOWN';
  if (cause) e.cause = cause;
  return e;
}

function __splitUtf8ChunksForDp__(input, maxBytes) {
  var s = String(input == null ? '' : input);
  var limit = Number(maxBytes);
  if (!Number.isFinite(limit) || limit < 16) {
    throw __mkMonitorPersistError__('MONITOR_V2_CHUNK_BUILD_FAILED', 'invalid chunk byte limit');
  }

  var chunks = [];
  var start = 0;
  var bytes = 0;

  for (var i = 0; i < s.length; i++) {
    var c = s.charCodeAt(i);
    var charBytes = 1;

    if (c <= 0x7F) {
      charBytes = 1;
    } else if (c <= 0x7FF) {
      charBytes = 2;
    } else if (c >= 0xD800 && c <= 0xDBFF) {
      // Surrogate pair -> treat as 4-byte UTF-8 sequence.
      charBytes = 4;
    } else {
      charBytes = 3;
    }

    if (bytes + charBytes > limit) {
      if (i <= start) {
        throw __mkMonitorPersistError__('MONITOR_V2_CHUNK_BUILD_FAILED', 'single codepoint exceeds chunk limit');
      }
      chunks.push(s.substring(start, i));
      start = i;
      bytes = 0;
    }

    bytes += charBytes;

    if (c >= 0xD800 && c <= 0xDBFF) {
      i++; // consume low surrogate in same chunk
    }
  }

  if (start < s.length) chunks.push(s.substring(start));
  if (!chunks.length) chunks.push('');
  return chunks;
}

function __dpSetMonitorFullV2__(dp, keyBase, fullObj, options) {
  var opts = options || {};
  var enableRawShadow = (opts.rawShadowEnabled === true);
  var base = String(keyBase || 'MONITOR_SNAPSHOT_FULL');

  var limit = 8500;
  try {
    if (globalThis.GLOBAL_CONFIG && GLOBAL_CONFIG.Render && GLOBAL_CONFIG.Render.DP && Number(GLOBAL_CONFIG.Render.DP.MAX_BYTES)) {
      limit = Number(GLOBAL_CONFIG.Render.DP.MAX_BYTES);
    }
  } catch (_) {}

  var payload;
  try {
    payload = JSON.stringify(fullObj || {});
  } catch (e) {
    throw __mkMonitorPersistError__('MONITOR_V2_SERIALIZE_FAILED', 'unable to serialize FULL snapshot', e);
  }

  var payloadBytes = __utf8Bytes__(payload);
  var chunks = __splitUtf8ChunksForDp__(payload, limit);
  var manifestKey = base + '_MANIFEST_V2';
  var partKeys = [];

  try { Logger.log('[MONITOR_V2][INFO] WRITE_START key=' + base + ' bytes=' + payloadBytes + ' chunkCount=' + chunks.length); } catch (_) {}

  for (var i = 0; i < chunks.length; i++) {
    var partKey = base + '_P' + (i + 1) + '_V2';
    var part = chunks[i];
    try {
      dp.setProperty(partKey, part);
      partKeys.push(partKey);
    } catch (ePart) {
      throw __mkMonitorPersistError__('MONITOR_V2_WRITE_FAILED', 'failed writing part ' + (i + 1), ePart);
    }
  }

  if (enableRawShadow) {
    try { Logger.log('[MONITOR_V2][WARN] SHADOW_RAW_ENABLED key=' + base); } catch (_) {}
    try {
      dp.setProperty(base, payload);
    } catch (eRaw) {
      throw __mkMonitorPersistError__('MONITOR_V2_WRITE_FAILED', 'failed writing shadow raw payload', eRaw);
    }
  }

  var manifest = {
    schema: 'monitor_snapshot_chunk_manifest',
    version: 2,
    snapshotType: 'FULL',
    parts: partKeys,
    partCount: partKeys.length,
    createdAt: new Date().toISOString()
  };

  if (manifest.partCount !== manifest.parts.length) {
    throw __mkMonitorPersistError__('MONITOR_V2_MANIFEST_COMMIT_FAILED', 'manifest invariant failed (partCount mismatch)');
  }

  try {
    dp.setProperty(manifestKey, JSON.stringify(manifest));
  } catch (eManifest) {
    throw __mkMonitorPersistError__('MONITOR_V2_MANIFEST_COMMIT_FAILED', 'failed writing manifest commit marker', eManifest);
  }

  try { Logger.log('[MONITOR_V2][INFO] MANIFEST_COMMIT_OK key=' + manifestKey + ' partCount=' + manifest.partCount); } catch (_) {}
  try { Logger.log('[MONITOR_V2][INFO] WRITE_OK key=' + base + ' partCount=' + manifest.partCount + ' bytes=' + payloadBytes + ' shadowRaw=' + enableRawShadow); } catch (_) {}

  return {
    ok: true,
    source: 'v2_chunked_writer',
    keyBase: base,
    manifestKey: manifestKey,
    partCount: manifest.partCount,
    bytesTotal: payloadBytes,
    shadowRaw: enableRawShadow
  };
}

/**
 * Chunked DP writer for large JSON values.
 * For objects with top-level data keys, writes each subkey as its own JSON part to avoid partial JSON slices.
 * Each part is a valid JSON document. Manifest records the keys and lengths.
 * Returns {ok, parts, total}.
 */
function __dpSetFullChunked__(dp, keyBase, fullObj) {
  try {
    // If object has top-level data keys, write per subkey as its own JSON object
    if (fullObj && typeof fullObj === 'object' && fullObj.data && typeof fullObj.data === 'object') {
      var manifest = { ver: '1.1', parts: [], bytes: [], ts: new Date().toISOString() };
      Object.keys(fullObj.data).forEach(function(subKey, idx) {
        var partKey = keyBase + '_P' + (idx + 1) + '_V1';
        var partObj = {
          timestamp: fullObj.timestamp || new Date().toISOString(),
          period: subKey,
          data: fullObj.data[subKey]
        };
        var json = JSON.stringify(partObj);
        dp.setProperty(partKey, json);
        manifest.parts.push(partKey);
        manifest.bytes.push(json.length);
      });
      dp.setProperty(keyBase + '_MANIFEST_V1', JSON.stringify(manifest));
      Logger.log('[DP][OK] ' + keyBase + ' parts=' + manifest.parts.length + ' [structured per subkey]');
      return { ok: true, parts: manifest.parts.length, total: manifest.bytes.reduce(function(a, b) { return a + b; }, 0) };
    }

    // Fallback: write as single JSON
    var s = JSON.stringify(fullObj || {});
    dp.setProperty(keyBase, s);
    Logger.log('[DP][OK] ' + keyBase + ' single object write, bytes=' + s.length);
    return { ok: true, parts: 1, total: s.length };
  } catch (e) {
    Logger.log('[DP][ERR] structured chunked write failed for ' + keyBase + ': ' + (e && e.message ? e.message : e));
    return { ok: false, error: e && e.message ? e.message : e };
  }
}

/**
 * Seed library state cache from PersistedState.trendHistoryMap so SB.buildFull can read trend history (SSoT).
 * Returns the current state cache object or null if unavailable.
 */

function __ensureStateCacheSeed__(cfg, candleCache){
  try {
    var getSC = (globalThis.RisontisCoreLibrary && RisontisCoreLibrary.TradeManager && RisontisCoreLibrary.TradeManager.getCurrentStateCache_) || globalThis.getCurrentStateCache_;
    var setSC = (globalThis.RisontisCoreLibrary && RisontisCoreLibrary.TradeManager && RisontisCoreLibrary.TradeManager.setCurrentStateCache_) || globalThis.setCurrentStateCache_;
    var st = (typeof getSC === 'function') ? getSC() : null;
    if (!st || typeof st !== 'object') { st = Object.create(null); if (typeof setSC === 'function') setSC(st); }
    var map = (cfg && cfg.PersistedState && cfg.PersistedState.trendHistoryMap) || {};
    var assets = Array.isArray(cfg && cfg.assets) ? cfg.assets : [];
    for (var i=0;i<assets.length;i++){
      var a = assets[i] && assets[i].Asset; if (!a || a.toUpperCase()==='GLOBAL') continue;
      st[a] = st[a] || { asset:String(a), trend:{ history:'' } };
      st[a].trend = st[a].trend || {};
      var tok = String(map[a] || '');
      st[a].trend.history = tok;
    }
    return st;
  } catch(_){ return null; }
}

/**
 * Merge trendHistory updates from __stateUpdates into PersistedState (tail-20),
 * using previously persisted FOURH_TREND_HISTORY_MAP from DP (prevAll).
 */
function __mergeTrendHistoryIntoPersisted__(cfg, prevAll){
  try {
    cfg.PersistedState = cfg.PersistedState || {};
    var prev = {};
    if (prevAll && typeof prevAll === 'object') {
      try { prev = JSON.parse(prevAll['FOURH_TREND_HISTORY_MAP'] || '{}'); } catch(_) { prev = {}; }
    }
    var up  = (cfg.__stateUpdates && cfg.__stateUpdates.trendHistoryMap) || {};
    var tailN = (cfg && cfg.Render && cfg.Render.Monitor && Number(cfg.Render.Monitor.TREND_TF_TAIL)) || 20;
    var out = Object.assign({}, prev);
    Object.keys(up).forEach(function(k){
      var tok = String(up[k] || '');
      out[k] = tok.length > tailN ? tok.slice(-tailN) : tok;
    });
    cfg.PersistedState.trendHistoryMap = out;
    return out;
  } catch(_){ return {}; }
}


/**
 * 5m trading cycle entrypoint (trigger target).
 */
function RunTradingSystem() {
  var __rtsTelemetry = {
    run_id: null,
    ts_start: null,
    ok: false,
    error: null,
    t0_entry: Date.now(),
    t1_lock_acquired: -1,
    t2_first_sheet_io: -1,
    t3_lib_start: -1,
    t4_lib_end: -1,
    t5_persist_start: -1,
    t6_persist_end: -1,
    t7_obs_start: -1,
    t8_obs_end: -1,
    t9_release: -1,
    t10_end: -1,
    persist_ms: -1,
    monitor_flush_ms: -1,
    systemlog_flush_ms: -1
  };
  try {
    __rtsTelemetry.run_id = Utilities.getUuid();
  } catch (_) {
    __rtsTelemetry.run_id = 'rts-' + String(__rtsTelemetry.t0_entry);
  }
  try {
    __rtsTelemetry.ts_start = new Date(__rtsTelemetry.t0_entry).toISOString();
  } catch (_) {
    __rtsTelemetry.ts_start = null;
  }
  function __rtsDeltaMs_(endTs, startTs) {
    if (!Number.isFinite(endTs) || !Number.isFinite(startTs) || startTs < 0 || endTs < 0) return -1;
    return Number(endTs - startTs);
  }
  function __rtsTimingEnabled_(cfgRef) {
    var c = cfgRef || cfg || globalThis.CURRENT_RUNTIME_CONFIG || ((typeof GLOBAL_CONFIG !== 'undefined') ? GLOBAL_CONFIG : null);
    try {
      var lvl = '';
      if (c && c.Logging && typeof c.Logging.logLevel === 'string') {
        lvl = c.Logging.logLevel;
      } else if (c && c.Logging && typeof c.Logging.GlobalLogLevel === 'string') {
        lvl = c.Logging.GlobalLogLevel;
      } else if (c && c.System && typeof c.System.LOG_LEVEL === 'string') {
        lvl = c.System.LOG_LEVEL;
      }
      return /^(INFO|DEBUG)$/.test(String(lvl || '').toUpperCase());
    } catch (_) {
      return false;
    }
  }
  function __emitRtsTiming_(cfgRef) {
    if (!__rtsTimingEnabled_(cfgRef)) return;
    var assetsCount = -1;
    try {
      if (cfgRef && Array.isArray(cfgRef.assets)) assetsCount = cfgRef.assets.length;
      else if (cfg && Array.isArray(cfg.assets)) assetsCount = cfg.assets.length;
    } catch (_) {}
    var payload = {
      run_id: __rtsTelemetry.run_id,
      ts_start: __rtsTelemetry.ts_start,
      ok: !!__rtsTelemetry.ok,
      error: __rtsTelemetry.error,
      lock_kind: 'script',
      assets_count: Number(assetsCount),
      lock_wait_ms: __rtsDeltaMs_(__rtsTelemetry.t1_lock_acquired, __rtsTelemetry.t0_entry),
      lock_hold_ms: __rtsDeltaMs_(__rtsTelemetry.t9_release, __rtsTelemetry.t1_lock_acquired),
      first_sheet_io_ms: __rtsDeltaMs_(__rtsTelemetry.t2_first_sheet_io, __rtsTelemetry.t1_lock_acquired),
      library_trading_ms: __rtsDeltaMs_(__rtsTelemetry.t4_lib_end, __rtsTelemetry.t3_lib_start),
      postrun_observability_ms: __rtsDeltaMs_(__rtsTelemetry.t8_obs_end, __rtsTelemetry.t5_persist_start),
      total_ms: __rtsDeltaMs_(__rtsTelemetry.t10_end, __rtsTelemetry.t0_entry),
      persist_ms: Number(__rtsTelemetry.persist_ms),
      monitor_flush_ms: Number(__rtsTelemetry.monitor_flush_ms),
      systemlog_flush_ms: Number(__rtsTelemetry.systemlog_flush_ms)
    };
    try { Logger.log('[RTS_TIMING] ' + JSON.stringify(payload)); } catch (_) {}
  }
  // Acquire global script lock to prevent overlapping trigger runs (blocking up to 5 minutes)
  var lock = LockService.getScriptLock();
  lock.waitLock(300000);
  __rtsTelemetry.t1_lock_acquired = Date.now();
  Logger.log('[Bootstrap] RunTradingSystem: lock acquired (waitLock up to 5 min).');
  var cfg = null;
  var licStatus = null;
  var refreshedLicenseJson = null;
  function logCapitalEvent_(msg, cfgRef) {
    try { Logger.log(msg); } catch (_) {}
    try {
      if (typeof logMessage === 'function') {
        logMessage('INFO', msg, null, cfgRef || cfg);
      }
    } catch (_) {}
  }
  function formatCapitalNumber_(n) {
    return Number.isFinite(n) ? Number(n).toFixed(2) : 'unset';
  }
  function formatCapitalDelta_(delta) {
    if (!Number.isFinite(delta)) return 'n/a';
    return (delta >= 0 ? '+' : '') + delta.toFixed(2);
  }
  function logDeployableCapitalMutation_(prevVal, nextVal, bracket, cfgRef) {
    var prevLabel = formatCapitalNumber_(prevVal);
    var nextLabel = formatCapitalNumber_(nextVal);
    var delta = Number.isFinite(prevVal) ? (nextVal - prevVal) : nextVal;
    var deltaLabel = formatCapitalDelta_(delta);
    var capLabel = Number.isFinite(bracket) ? String(bracket) : 'n/a';
    var msg = '[CAPITAL] DEPLOYABLE_CAPITAL_EUR updated: ' + prevLabel + ' -> ' + nextLabel +
      ' (delta=' + deltaLabel + ', cap=' + capLabel + ')';
    logCapitalEvent_(msg, cfgRef);
  }
  try {
    requireGlobalConfig_();
    assertNoScriptProperties_();

    var dp = PropertiesService.getDocumentProperties();
    var nowTs = Date.now();
    var ttlMs = 24 * 60 * 60 * 1000;
    var lastRaw = dp.getProperty('LICENSE_LAST_REFRESH_AT');
    var lastTs = Number(lastRaw);
    var ttlExpired = !isFinite(lastTs) || lastTs <= 0 || (nowTs - lastTs) > ttlMs;

    var retryRaw = dp.getProperty('LICENSE_REFRESH_RETRY_COUNT');
    var retryCount = parseInt(retryRaw, 10);
    if (!isFinite(retryCount) || retryCount < 0) retryCount = 0;

    var syncFn = (typeof hostSyncLicense_ === 'function')
      ? hostSyncLicense_
      : ((typeof hostSyncLicense === 'function') ? hostSyncLicense : null);

    if (ttlExpired && syncFn) {
      Logger.log('[Bootstrap][LicenseRefresh] attempt (ttl)');
      var ttlRes = syncFn();
      dp.setProperty('LICENSE_LAST_REFRESH_AT', String(nowTs));
      if (ttlRes && ttlRes.ok === true) {
        dp.setProperty('LICENSE_REFRESH_RETRY_COUNT', '0');
        refreshedLicenseJson = dp.getProperty('LICENSE_JSON') || null;
        Logger.log('[Bootstrap][LicenseRefresh] success (ttl)');
      }
    }

    if (typeof hostEnsureVerifiedLicense === 'function') {
      licStatus = hostEnsureVerifiedLicense();
      if (!licStatus || licStatus.ok !== true) {
        var retryRawGuard = dp.getProperty('LICENSE_REFRESH_RETRY_COUNT');
        var retryCountGuard = parseInt(retryRawGuard, 10);
        if (!isFinite(retryCountGuard) || retryCountGuard < 0) retryCountGuard = 0;
        if (retryCountGuard >= 3 || !syncFn) {
          if (retryCountGuard >= 3) Logger.log('[Bootstrap][LicenseRefresh] skip (retry>=3)');
        } else {
          Logger.log('[Bootstrap][LicenseRefresh] attempt (guard-failed)');
          var res = syncFn();
          if (res && res.ok === true) {
            refreshedLicenseJson = dp.getProperty('LICENSE_JSON') || null;
          }
          licStatus = hostEnsureVerifiedLicense();
          if (licStatus && licStatus.ok === true) {
            dp.setProperty('LICENSE_REFRESH_RETRY_COUNT', '0');
            dp.setProperty('LICENSE_LAST_REFRESH_AT', String(nowTs));
            Logger.log('[Bootstrap][LicenseRefresh] success (guard-failed)');
          } else {
            dp.setProperty('LICENSE_REFRESH_RETRY_COUNT', String(retryCountGuard + 1));
          }
        }
      }
      if (!licStatus || licStatus.ok !== true) {
        var msg = (licStatus && licStatus.issues && licStatus.issues.length)
          ? licStatus.issues.join(', ')
          : 'License missing: please start a trial or activate a subscription.';
        throw new Error(msg);
      } else if (Number(dp.getProperty('LICENSE_REFRESH_RETRY_COUNT') || 0) > 0) {
        dp.setProperty('LICENSE_REFRESH_RETRY_COUNT', '0');
      }
    } else {
      throw new Error('License enforcement unavailable (hostEnsureVerifiedLicense missing).');
    }

  // 1) Build base config and inject credentials
  cfg = buildConfigForRun_();
  cfg = injectRuntimeCredsFromUP_(cfg);

  // 2) Resolve settings (with Settings sheet hash — reuse if hash unchanged)
  try {
    var dp = PropertiesService.getDocumentProperties();
    if (__rtsTelemetry.t2_first_sheet_io < 0) __rtsTelemetry.t2_first_sheet_io = Date.now();
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var settingsSheet = ss.getSheetByName('Settings');
    if (!settingsSheet) throw new Error('[Bootstrap] Settings sheet not found.');
    // Read A2:B (all rows below header)
    var settingsValues = settingsSheet.getRange(2, 1, Math.max(0, settingsSheet.getLastRow()-1), 2).getValues();
    // Flatten to string for hash (row-wise join with tab, then join rows with newline)
    var settingsStr = settingsValues.map(function(row){ return row.join('\t'); }).join('\n');
    // Compute MD5 hash (using Utilities)
    var settingsHash = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, settingsStr, Utilities.Charset.UTF_8)
      .map(function(b){return ('0'+(b&0xFF).toString(16)).slice(-2)}).join('');
    var prevHash = dp.getProperty('SETTINGS_HASH');
    // Disable settings cache for trading runs (always resolve fresh)
    cfg = callCoreFn_(['resolveSettingsImpl', 'resolveSettings'], cfg);
    globalThis.__RESOLVED_CFG__ = cfg;
    dp.setProperty('SETTINGS_HASH', settingsHash);
    Logger.log('[Bootstrap] Reloaded Settings (trading run – no cache)');
  } catch (e) {
    Logger.log('[Bootstrap] resolveSettings (hash) failed: ' + (e && e.message ? e.message : e));
    throw e;
  }

  // 3) Inject PersistedState from host DocumentProperties (SSOT)
  try {
    var dp  = PropertiesService.getDocumentProperties();
    var all = dp.getProperties(); // force fresh read
    if (refreshedLicenseJson) {
      all['LICENSE_JSON'] = refreshedLicenseJson;
    }
    var cooldowns = {}; var mdCache = {};
    try { cooldowns = JSON.parse(all['COOLDOWNS_V1'] || '{}'); } catch(_) {}
    try { mdCache   = JSON.parse(all['MARKETDATA_CACHE_V1'] || '{}'); } catch(_) {}
    cfg.PersistedState = {
      availableCash: all['AVAILABLE_CASH'] || "",
      dailyPnlPct:   all['DAILY_PNL_PCT'] || "",
      cooldowns:     cooldowns,
      marketDataCache: mdCache
    };
    var licenseJson = all['LICENSE_JSON'];
    if (licenseJson) {
      cfg.PersistedState.LICENSE_JSON = licenseJson;
      try {
        var licenseDto = null;
        try { licenseDto = JSON.parse(licenseJson); } catch (_) { licenseDto = null; }
        var sigOk = false;
        if (licenseDto && typeof verifyLicensePayload_ === 'function') {
          sigOk = !!verifyLicensePayload_(licenseDto);
        }
        cfg.PersistedState.LICENSE_SIGNATURE_OK = sigOk;
        var binding = (typeof buildLicenseBinding_ === 'function') ? buildLicenseBinding_() : null;
        if (binding && (binding.email || binding.sheetId)) {
          cfg.PersistedState.LICENSE_BINDING = binding;
        }
      } catch (e) {
        try { Logger.log('[Bootstrap] LICENSE_SIGNATURE_OK verify failed: ' + (e && e.message ? e.message : e)); } catch (_) {}
      }
    }
    // Phase 1 (Option A): deployable capital detection and persistence (no trading impact).
    try {
      var sc = Number(cfg && cfg.Risk && cfg.Risk.STARTING_CASH_EUR);
      if (Number.isFinite(sc)) {
        var bracket = null;
        try {
          if (licStatus && licStatus.license) {
            var lic = licStatus.license;
            if (lic && lic.type === 'trial') bracket = 5000;
            else if (lic.capitalBracket != null) bracket = Number(lic.capitalBracket);
          } else if (licStatus && licStatus.capitalBracket != null) {
            bracket = Number(licStatus.capitalBracket);
          }
        } catch (_) {}
        var effective = Number.isFinite(bracket) ? Math.min(sc, bracket) : sc;
        if (effective < 0) effective = 0;
        var depRaw = Number(all['DEPLOYABLE_CAPITAL_EUR']);
        if (!Number.isFinite(depRaw)) {
          dp.setProperty('DEPLOYABLE_CAPITAL_EUR', effective.toFixed(2));
          all['DEPLOYABLE_CAPITAL_EUR'] = effective.toFixed(2);
          logDeployableCapitalMutation_(depRaw, effective, bracket, cfg);
          depRaw = effective;
        } else if (effective > depRaw) {
          dp.setProperty('DEPLOYABLE_CAPITAL_EUR', effective.toFixed(2));
          all['DEPLOYABLE_CAPITAL_EUR'] = effective.toFixed(2);
          logDeployableCapitalMutation_(depRaw, effective, bracket, cfg);
          depRaw = effective;
        } else if (effective < depRaw) {
          var next = Math.max(effective, 0);
          dp.setProperty('DEPLOYABLE_CAPITAL_EUR', next.toFixed(2));
          all['DEPLOYABLE_CAPITAL_EUR'] = next.toFixed(2);
          logDeployableCapitalMutation_(depRaw, next, bracket, cfg);
          depRaw = next;
        }
        cfg.PersistedState.DEPLOYABLE_CAPITAL_EUR = depRaw;
      }
    } catch (_dep) {
      Logger.log('[Bootstrap] DEPLOYABLE_CAPITAL_EUR detection failed: ' + (_dep && _dep.message ? _dep.message : _dep));
    }
    // Inject host-owned license backoff state (read-only, host SSOT)
    try {
      var backoff = (typeof readLicenseBackoffState_ === 'function')
        ? readLicenseBackoffState_()
        : {
            LICENSE_BACKOFF_COUNT: all['LICENSE_BACKOFF_COUNT'],
            LICENSE_RETRY_AFTER: all['LICENSE_RETRY_AFTER']
          };
      if (backoff && typeof backoff === 'object') {
        if (typeof backoff.LICENSE_BACKOFF_COUNT !== 'undefined') {
          cfg.PersistedState.LICENSE_BACKOFF_COUNT = backoff.LICENSE_BACKOFF_COUNT;
        }
        if (typeof backoff.LICENSE_RETRY_AFTER !== 'undefined') {
          cfg.PersistedState.LICENSE_RETRY_AFTER = backoff.LICENSE_RETRY_AFTER;
        }
      }
    } catch (_) {}
    // Additional host flags used by SetupRunner (property-free library)
    cfg.PersistedState.credentialsVerified = (all['CREDENTIALS_VERIFIED'] === '1');
    cfg.PersistedState.maintenanceOngoing = (all['MaintenanceOngoing'] === 'true');

    // Inject last known equity from DP
    try {
      var eq = Number(all['LAST_EQUITY_EUR']);
      if (Number.isFinite(eq)) {
        cfg.PersistedState.equityEUR = eq;
        cfg.PersistedState.lastEquityDate = all['LAST_EQUITY_DATE'] || '';
        Logger.log('[Bootstrap] Injected LAST_EQUITY_EUR=' + eq);
      }
      // Ensure compatibility with TradeManager.ensureEquityState_
      if (Number.isFinite(eq)) {
        cfg.PersistedState.LAST_EQUITY_EUR = eq;
      }
      // Inject previous-day equity baseline if present
      const prevEq = Number(all['EQUITY_PREV_DAY_EUR']);
      if (Number.isFinite(prevEq)) {
        cfg.PersistedState.EQUITY_PREV_DAY_EUR = prevEq;
      }
      // NEW: inject previous-day equity date for TradeManager.calcRealtimeEquity_
      if (all['EQUITY_PREV_DAY_DATE']) {
        cfg.PersistedState.EQUITY_PREV_DAY_DATE = all['EQUITY_PREV_DAY_DATE'];
      }
      // Phase 2 (Option A): initialize fixed equity baseline once for sizing.
      var baseRaw = Number(all['EQUITY_BASELINE_EUR']);
      if (!Number.isFinite(baseRaw) && Number.isFinite(eq)) {
        dp.setProperty('EQUITY_BASELINE_EUR', eq.toFixed(2));
        all['EQUITY_BASELINE_EUR'] = eq.toFixed(2);
        logCapitalEvent_('[CAPITAL] EQUITY_BASELINE_EUR seeded from current equity (' + eq.toFixed(2) +
          '); no equity or PnL rebasing performed.', cfg);
        baseRaw = eq;
      }
      if (Number.isFinite(baseRaw)) {
        cfg.PersistedState.EQUITY_BASELINE_EUR = baseRaw;
      }
    } catch (_) {}

    // Inject trend/gap/ADX history maps for OrderMonitor (host-persisted JSON)
    try { cfg.PersistedState.trendHistoryMap  = JSON.parse(all['FOURH_TREND_HISTORY_MAP'] || '{}'); } catch(_) { cfg.PersistedState.trendHistoryMap  = {}; }
    // Inject last 4h timestamp map for trend append logic
    try { cfg.PersistedState.trendLast4hTsMap = JSON.parse(all['TREND_LAST4H_TS_MAP'] || '{}'); } catch(_) { cfg.PersistedState.trendLast4hTsMap = {}; }
    try { cfg.PersistedState.gapHistoryMap    = JSON.parse(all['GAP_HISTORY_MAP'] || '{}'); }           catch(_) { cfg.PersistedState.gapHistoryMap    = {}; }
    try { cfg.PersistedState.adxGapHistoryMap = JSON.parse(all['ADX_GAP_HISTORY_MAP'] || '{}'); }       catch(_) { cfg.PersistedState.adxGapHistoryMap = {}; }
    // Inject monitor overview for CC/DTO propagation (trading/cache status)
    try { cfg.PersistedState.monitorOverview = JSON.parse(all['MONITOR_OVERVIEW_V1'] || '{}'); } catch(_) { cfg.PersistedState.monitorOverview = {}; }
  } catch (ePS) {
    Logger.log('[Bootstrap] PersistedState inject failed: ' + (ePS && ePS.message ? ePS.message : ePS));
  }

  // Inject unified lastTradedMap (sheet + archive) via TradesService (SSOT)
  try {
    if (
      globalThis.RisontisCoreLibrary &&
      RisontisCoreLibrary.TradesService &&
      typeof RisontisCoreLibrary.TradesService.debugMergeSheetAndArchive_ === 'function' &&
      typeof RisontisCoreLibrary.TradesService.computeLastTradedMap_ === 'function'
    ) {
      // Use unified raw rows (sheet + archive) for lastTradedMap (SSOT)
      var dtoLT = hostGetTradesArchiveDto_();
      var mergedLT = RisontisCoreLibrary.TradesService.debugMergeSheetAndArchive_(dtoLT);
      var rawRowsLT = (mergedLT && mergedLT.data && Array.isArray(mergedLT.data.merged)) ? mergedLT.data.merged : [];
      cfg.PersistedState.lastTradedMap = RisontisCoreLibrary.TradesService.computeLastTradedMap_(rawRowsLT);
    } else {
      throw new Error('TradesService functions for lastTradedMap not available');
    }
  } catch(eLT) {
    Logger.log('[Bootstrap] lastTradedMap inject failed: ' + (eLT && eLT.message ? eLT.message : eLT));
    throw eLT;
  }

  // Validate persisted equity state before trading cycle
  try {
    const lastEq = cfg.PersistedState.LAST_EQUITY_EUR;
    const prevEq = cfg.PersistedState.EQUITY_PREV_DAY_EUR;
    const tradingEnabled = !!(cfg && cfg.Execution && cfg.Execution.TRADING_ENABLED === true);

    // Cold install: no equity persisted yet → initialize from STARTING_CASH_EUR
    if (!Number.isFinite(lastEq) && !Number.isFinite(prevEq)) {
      if (tradingEnabled) {
        const startingCash = Number(cfg.Risk && cfg.Risk.STARTING_CASH_EUR);
        if (!Number.isFinite(startingCash) || startingCash <= 0) {
          throw new Error('[Bootstrap] Cannot initialize equity: invalid STARTING_CASH_EUR');
        }

        Logger.log('[Bootstrap] Initializing equity state for trading start from STARTING_CASH_EUR=' + startingCash);

        cfg.PersistedState.LAST_EQUITY_EUR = startingCash;
        cfg.PersistedState.EQUITY_PREV_DAY_EUR = startingCash;
        cfg.PersistedState.lastEquityDate = '';
        cfg.PersistedState.EQUITY_PREV_DAY_DATE = '';

        // Persist immediately so downstream logic sees a consistent state
        PropertiesService.getDocumentProperties().setProperties({
          'LAST_EQUITY_EUR': String(startingCash),
          'EQUITY_PREV_DAY_EUR': String(startingCash)
        }, false);
      } else {
        try {
          if (typeof logMessage === 'function') {
            logMessage('INFO', '[EQUITY] Equity init skipped (TRADING_ENABLED=false; equity not started).', null, cfg);
          }
        } catch (_) {}
      }

    } else if (!Number.isFinite(lastEq) || !Number.isFinite(prevEq)) {
      // Partial state indicates corruption
      throw new Error('[Bootstrap] Inconsistent persisted equity state');
    }
  } catch (e) {
    Logger.log('[FATAL] ' + e.message);
    throw e; // hard fail: stop trading run
  }

  // One-time bootstrap: promote first realized equity into prev-day baseline (fresh installs only)
  try {
    const prevDayDate = cfg.PersistedState && cfg.PersistedState.EQUITY_PREV_DAY_DATE;
    const lastEq = Number(cfg.PersistedState && cfg.PersistedState.LAST_EQUITY_EUR);
    const startingCash = Number(cfg.Risk && cfg.Risk.STARTING_CASH_EUR);
    const prevDayMissing = !prevDayDate;
    if (prevDayMissing && Number.isFinite(lastEq) && Number.isFinite(startingCash) && lastEq !== startingCash) {
      const tz = (cfg && cfg.Logging && cfg.Logging.timeZone) ? cfg.Logging.timeZone : (Session.getScriptTimeZone() || 'Etc/UTC');
      const lastEqDate = (cfg.PersistedState && cfg.PersistedState.lastEquityDate) || '';
      const effectiveDate = lastEqDate || Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd');
      cfg.PersistedState.EQUITY_PREV_DAY_EUR = lastEq;
      cfg.PersistedState.EQUITY_PREV_DAY_DATE = effectiveDate;
      PropertiesService.getDocumentProperties().setProperties({
        'EQUITY_PREV_DAY_EUR': lastEq.toFixed(2),
        'EQUITY_PREV_DAY_DATE': effectiveDate
      }, false);
      Logger.log(`[Bootstrap] Promoted first realized equity to prev-day baseline → ${lastEq.toFixed(2)} @ ${effectiveDate}`);
    }
  } catch (e) {
    Logger.log('[Bootstrap] First-equity baseline promotion skipped: ' + (e && e.message ? e.message : e));
  }

  // 4) Run trading cycle in library
  globalThis.CURRENT_RUNTIME_CONFIG = cfg;
  Logger.log('[Bootstrap] RunTradingSystem: forwarding cfg with assets=' + (cfg.assets ? cfg.assets.length : 0));
  __rtsTelemetry.t3_lib_start = Date.now();
  var runResult;
  try {
    runResult = callCoreFn_(['run', 'RunTradingSystem'], cfg);
  } finally {
    __rtsTelemetry.t4_lib_end = Date.now();
  }

  // 5) Persist snapshot & state updates to host DP (SSOT)
  __rtsTelemetry.t5_persist_start = Date.now();
  try {
    var dp2  = PropertiesService.getDocumentProperties();
    var all2 = dp2.getProperties();

    // Snapshot prepared by TM (property-free library)
    var snap = cfg.__preparedSnapshot || globalThis.__LAST_MONITOR_SNAPSHOT__;
    if (snap) {
      // Totals from prepared snapshot (host stays lean; library is SSOT for per-asset open counts)
      try {
        var totalOpen = 0;
        if (snap && Array.isArray(snap.assets)) {
          for (var i = 0; i < snap.assets.length; i++) {
            var a = snap.assets[i];
            totalOpen += Number((a && a.openTrades) ? a.openTrades : 0);
          }
        }
        if (snap.totals && typeof snap.totals === 'object') {
          snap.totals.openTrades = totalOpen;
        }
      } catch (_) {}


      // FULL snapshot: persist via canonical V2 chunked writer (manifest-driven SSOT)
      try {
        var fullDto = cfg.__preparedFullSnapshot;
        if (!fullDto) {
          throw __mkMonitorPersistError__('MONITOR_V2_FULL_DTO_MISSING', 'preparedFullSnapshot missing from TradeManager');
        }

        // Temporary compatibility toggle (default false): writes legacy raw shadow only when explicitly enabled.
        var rawShadowEnabled = !!(cfg && cfg.Settings && cfg.Settings.MONITOR_FULL_RAW_SHADOW_COMPAT === true);
        var monitorWriteResult = __dpSetMonitorFullV2__(dp2, 'MONITOR_SNAPSHOT_FULL', fullDto, {
          rawShadowEnabled: rawShadowEnabled
        });

        if (!monitorWriteResult || monitorWriteResult.ok !== true) {
          throw __mkMonitorPersistError__('MONITOR_V2_WRITE_FAILED', 'writer returned non-ok result');
        }
      } catch (e) {
        if (!(e && e.code)) {
          throw __mkMonitorPersistError__('MONITOR_V2_WRITE_FAILED', 'monitor FULL persistence failed', e);
        }
        throw e;
      }
    }

    // State updates (cooldowns) if TM exposed them on the config in this run
    var updates = cfg.__stateUpdates || {};
    // Batch-write optimization: collect all simple setProperty calls into one object
    var toWrite = {};
    // Persist monitor stats provided by library (property-free TM)
    if (typeof updates.availableCash !== 'undefined') {
      toWrite['AVAILABLE_CASH'] = String(updates.availableCash);
    }
    if (typeof updates.dailyPnlPct !== 'undefined') {
      toWrite['DAILY_PNL_PCT'] = String(updates.dailyPnlPct);
    }
    // Persist equity updates if present
    if (typeof updates.LAST_EQUITY_EUR !== 'undefined') {
      toWrite['LAST_EQUITY_EUR'] = String(updates.LAST_EQUITY_EUR);
    }
    if (typeof updates.LAST_EQUITY_DATE !== 'undefined') {
      toWrite['LAST_EQUITY_DATE'] = String(updates.LAST_EQUITY_DATE);
    }
    // NEW: persist rolling equity buffer (14-day window) if present
    if (typeof updates.EQUITY_BUFFER_V1 !== 'undefined') {
      toWrite['EQUITY_BUFFER_V1'] = String(updates.EQUITY_BUFFER_V1);
    }
    if (typeof updates.credentialsVerified !== 'undefined') {
      toWrite['CREDENTIALS_VERIFIED'] = String(updates.credentialsVerified);
    }
    if (typeof updates.maintenanceOngoing !== 'undefined') {
      toWrite['MaintenanceOngoing'] = String(updates.maintenanceOngoing);
    }
    if (typeof updates.exchangeProbeFailCount !== 'undefined') {
      toWrite['EXCHANGE_PROBE_FAIL_COUNT'] = String(updates.exchangeProbeFailCount);
    }
    if (typeof updates.exchangeProbeFailTs !== 'undefined') {
      toWrite['EXCHANGE_PROBE_FAIL_TS'] = String(updates.exchangeProbeFailTs);
    }
    // Commit all simple DP writes in a single call
    if (Object.keys(toWrite).length > 0) {
      __dpBatchWrite__(dp2, toWrite);
    }
    // Persist cooldowns (merged)
    if (updates.cooldowns && Object.keys(updates.cooldowns).length) {
      var merged = {};
      try { merged = JSON.parse(all2['COOLDOWNS_V1'] || '{}'); } catch(_) {}
      Object.assign(merged, updates.cooldowns);
      __dpSetJsonGuarded__(dp2, 'COOLDOWNS_V1', merged);
      try { Logger.log('[Bootstrap] COOLDOWNS_V1 merged (' + Object.keys(updates.cooldowns).length + ' key(s))'); } catch(_) {}
    }

    // Persist market data cache (map) if provided by library (merge maps)
    if (updates.marketDataCache && typeof updates.marketDataCache === 'object' && updates.marketDataCache.map) {
      try {
        var mdPrev = {};
        try { mdPrev = JSON.parse(all2['MARKETDATA_CACHE_V1'] || '{}'); } catch(_) {}
        if (!mdPrev || typeof mdPrev !== 'object') mdPrev = {};
        if (!mdPrev.map) mdPrev.map = {};
        if (typeof updates.marketDataCache.ts === 'number') mdPrev.ts = updates.marketDataCache.ts;
        Object.assign(mdPrev.map, updates.marketDataCache.map);
        __dpSetJsonGuarded__(dp2, 'MARKETDATA_CACHE_V1', mdPrev);
        try { Logger.log('[Bootstrap] MARKETDATA_CACHE_V1 merged (' + Object.keys(updates.marketDataCache.map).length + ' key(s))'); } catch(_) {}
      } catch (_mdErr) {}
    }

    // Persist trend/gap/ADX history maps from OrderMonitor (merge maps)
    // Persist 4h trend history (FTR: must be provided by TradeManager; no fallback)
    if (!updates.trendHistoryMap || !Object.keys(updates.trendHistoryMap).length) {
      throw new Error('[TREND][FTR] trendHistoryMap missing in __stateUpdates – cannot persist 4h trend history.');
    }
    __dpSetJsonGuarded__(dp2, 'FOURH_TREND_HISTORY_MAP', updates.trendHistoryMap);
    if (updates.gapHistoryMap && typeof updates.gapHistoryMap === 'object') {
      try {
        var ghPrev = {}; try { ghPrev = JSON.parse(all2['GAP_HISTORY_MAP'] || '{}'); } catch(_) {}
        Object.assign(ghPrev, updates.gapHistoryMap);
        __dpSetJsonGuarded__(dp2, 'GAP_HISTORY_MAP', ghPrev);
      } catch (_) {}
    }
    if (updates.adxGapHistoryMap && typeof updates.adxGapHistoryMap === 'object') {
      try {
        var ahPrev = {}; try { ahPrev = JSON.parse(all2['ADX_GAP_HISTORY_MAP'] || '{}'); } catch(_) {}
        Object.assign(ahPrev, updates.adxGapHistoryMap);
        __dpSetJsonGuarded__(dp2, 'ADX_GAP_HISTORY_MAP', ahPrev);
      } catch (_) {}
    }

    // Persist previous gaps for mini/full HTML monitor parity (merge maps)
    if (updates.prevGapHistoryMap && typeof updates.prevGapHistoryMap === 'object') {
      try {
        var pghPrev = {}; try { pghPrev = JSON.parse(all2['PREV_GAP_HISTORY_MAP'] || '{}'); } catch(_) {}
        Object.assign(pghPrev, updates.prevGapHistoryMap);
        __dpSetJsonGuarded__(dp2, 'PREV_GAP_HISTORY_MAP', pghPrev);
      } catch (_) {}
    }
    if (updates.prevAdxGapHistoryMap && typeof updates.prevAdxGapHistoryMap === 'object') {
      try {
        var pahPrev = {}; try { pahPrev = JSON.parse(all2['PREV_ADX_GAP_HISTORY_MAP'] || '{}'); } catch(_) {}
        Object.assign(pahPrev, updates.prevAdxGapHistoryMap);
        __dpSetJsonGuarded__(dp2, 'PREV_ADX_GAP_HISTORY_MAP', pahPrev);
      } catch (_) {}
    }

    // Persist normalized direction tokens for EMA/ADX (SSOT for HTML monitors)
    if (updates.emaDirMap && typeof updates.emaDirMap === 'object') {
      try {
        var emaDirPrev = {}; try { emaDirPrev = JSON.parse(all2['EMA_DIR_MAP'] || '{}'); } catch(_) {}
        Object.assign(emaDirPrev, updates.emaDirMap);
        __dpSetJsonGuarded__(dp2, 'EMA_DIR_MAP', emaDirPrev);
      } catch (_) {}
    }
    if (updates.adxDirMap && typeof updates.adxDirMap === 'object') {
      try {
        var adxDirPrev = {}; try { adxDirPrev = JSON.parse(all2['ADX_DIR_MAP'] || '{}'); } catch(_) {}
        Object.assign(adxDirPrev, updates.adxDirMap);
        __dpSetJsonGuarded__(dp2, 'ADX_DIR_MAP', adxDirPrev);
      } catch (_) {}
    }

    // Persist monitor overview (trading/cache status) for SB/CC
    if (updates.monitorOverview && typeof updates.monitorOverview === 'object') {
      __dpSetJsonGuarded__(dp2, 'MONITOR_OVERVIEW_V1', updates.monitorOverview);
    }

    // Helpful seeds to avoid exposure/limit edge cases on first run
    if (!all2['AVAILABLE_CASH'] && typeof cfg.Settings?.STARTING_CASH_EUR !== 'undefined') {
      dp2.setProperty('AVAILABLE_CASH', String(cfg.Settings.STARTING_CASH_EUR));
    }
    if (!all2['DAILY_PNL_PCT']) {
      dp2.setProperty('DAILY_PNL_PCT', '0');
    }

    // Clear the in-memory state update queue to prevent duplicate writes on next run
    try { cfg.__stateUpdates = {}; } catch (_) {}
  } catch (ePersist) {
    var pCode = (ePersist && ePersist.code) ? String(ePersist.code) : '';
    if (pCode && pCode.indexOf('MONITOR_V2_') === 0) {
      try { Logger.log('[MONITOR_V2][ERROR] MONITOR_V2_PERSIST_ABORTED code=' + pCode + ' message=' + (ePersist && ePersist.message ? ePersist.message : ePersist)); } catch(_) {}
      throw __mkMonitorPersistError__('MONITOR_V2_PERSIST_ABORTED', 'aborting run due to monitor FULL persist failure', ePersist);
    }
    try { Logger.log('[Bootstrap] Persist to host DP failed: ' + (ePersist && ePersist.message ? ePersist.message : ePersist)); } catch(_) {}
  }
  __rtsTelemetry.t6_persist_end = Date.now();
  __rtsTelemetry.persist_ms = __rtsDeltaMs_(__rtsTelemetry.t6_persist_end, __rtsTelemetry.t5_persist_start);

  // No flat monitor snapshot/fallbacks written; only MONITOR_SNAPSHOT_FULL is persisted.

  // 6) Deterministic host flush after each trading cycle (FTR, lean, single source of truth)
  __rtsTelemetry.t7_obs_start = Date.now();
  try {
    var __mwStart = Date.now();
    Logger.log('[Bootstrap] Performing deterministic post-run host flush (5m + 4h).');
    runMonitorWriterTrigger(); // Always flush buffers once per completed trading cycle
    __rtsTelemetry.monitor_flush_ms = __rtsDeltaMs_(Date.now(), __mwStart);
    // Host-level SystemLog flush via CoreExports (module-based)
    try {
      var __sysStart = Date.now();
      callCoreFn_(['flushSystemLogToSheet'], cfg);
      __rtsTelemetry.systemlog_flush_ms = __rtsDeltaMs_(Date.now(), __sysStart);
      Logger.log('[Bootstrap] Host-level SystemLog flush completed.');
    } catch (eFlush) {
      Logger.log('[Bootstrap] SystemLog flush unavailable: ' + (eFlush && eFlush.message ? eFlush.message : eFlush));
    }
  } catch (eFlush) {
    Logger.log('[Bootstrap] Post-run host flush failed: ' + (eFlush && eFlush.message ? eFlush.message : eFlush));
  }
  __rtsTelemetry.t8_obs_end = Date.now();

    __rtsTelemetry.ok = true;
    return runResult;
  } catch (eRun) {
    __rtsTelemetry.ok = false;
    __rtsTelemetry.error = (eRun && eRun.message) ? String(eRun.message) : String(eRun);
    try { logExecutionFailure_(eRun); } catch (_) {}
    throw eRun;
  } finally {
    var __releaseStart = Date.now();
    try { lock.releaseLock(); } catch (_e) {}
    __rtsTelemetry.t9_release = Date.now();
    // Explicit memory cleanup to stabilize runtimes
    // Post-run cleanup to keep memory stable
    try {
      delete globalThis.__LAST_MONITOR_SNAPSHOT__;
      delete globalThis.CURRENT_RUNTIME_CONFIG;
      if (globalThis.RisontisCoreLibrary &&
          globalThis.RisontisCoreLibrary.TradeManager &&
          typeof globalThis.RisontisCoreLibrary.TradeManager.clearCaches === 'function') {
        globalThis.RisontisCoreLibrary.TradeManager.clearCaches();
      }
    } catch (_) {}
    if (__rtsTelemetry.t9_release < 0) {
      __rtsTelemetry.t9_release = __releaseStart;
    }
    __rtsTelemetry.t10_end = Date.now();
    __emitRtsTiming_(cfg);
  }
}


/** Daily system log rotation (trigger target). */
function rotateSystemLogSheet() {
  requireGlobalConfig_();
  assertNoScriptProperties_();
  var cfg = buildConfigForRun_();
  cfg = injectRuntimeCredsFromUP_(cfg);
  try {
    cfg = ensureLoggingDefaults_(cfg);
    try {
      cfg = RisontisCoreLibrary.ConfigUtils.validateConfigLite(cfg);
      Logger.log('[Bootstrap] Performed light config validation for non-trading task.');
    } catch (e) {
      Logger.log('[Bootstrap] validateConfigLite failed: ' + (e && e.message ? e.message : e));
      throw e;
    }
  } catch (e) {
    throw new Error('[Bootstrap] resolveSettings (library) failed early: ' + (e && e.message ? e.message : e));
  }
  // Always validate config before invoking the core function
  try {
    callCoreFn_(['validateConfig','ValidateConfig'], cfg);
  } catch (_e) {
    cfg = ensureLoggingDefaults_(cfg);
  }
  globalThis.CURRENT_RUNTIME_CONFIG = cfg;
  Logger.log('[Bootstrap] rotateSystemLogSheet: forwarding cfg with assets=' + (cfg.assets ? cfg.assets.length : 0));
  assertRoutingConfig_(cfg);
  const result = callCoreFn_(['rotateSystemLogSheetImpl','rotateSystemLogSheet','rotateSystemLog'], cfg);
  try {
    runSheetStyling();
  } catch (e) {
    Logger.log('[Bootstrap] runSheetStyling after rotation failed: ' + (e && e.message ? e.message : e));
  }
  return result;
}

/** Equity rotator job (trigger target, SSOT host-persist discipline). */
function runEquityRotator() {
  requireGlobalConfig_();
  assertNoScriptProperties_();

  // 1. Build base config and inject credentials
  var cfg = buildConfigForRun_();
  cfg = injectRuntimeCredsFromUP_(cfg);

  // 2. Ensure logging defaults (tz + format) and perform light validation
  try {
    cfg = ensureLoggingDefaults_(cfg);
    try {
      cfg = RisontisCoreLibrary.ConfigUtils.validateConfigLite(cfg);
      Logger.log('[Bootstrap] Performed light config validation for non-trading task.');
    } catch (e) {
      Logger.log('[Bootstrap] validateConfigLite failed: ' + (e && e.message ? e.message : e));
      throw e;
    }
  } catch (e) {
    throw new Error('[Bootstrap] resolveSettings (library) failed early: ' + (e && e.message ? e.message : e));
  }

  // Hydrate PersistedState with last and previous equity values from DP (host is SSOT)
  const dp = PropertiesService.getDocumentProperties();
  const all = dp.getProperties();
  const lastEq = Number(all['LAST_EQUITY_EUR']);
  const prevEq = Number(all['EQUITY_PREV_DAY_EUR']);
  const prevDate = all['EQUITY_PREV_DAY_DATE'] || '';
  const prevBuffer = all['EQUITY_BUFFER_V1'] || '';

  if (!Number.isFinite(lastEq)) {
    throw new Error('[Bootstrap] Missing LAST_EQUITY_EUR in DocumentProperties; aborting equity rotation.');
  }

  // Construct PersistedState for SSOT equity rotation model (host is canonical)
  cfg.PersistedState = {
    LAST_EQUITY_EUR: lastEq,
    EQUITY_PREV_DAY_EUR: Number.isFinite(prevEq) ? prevEq : lastEq,
    EQUITY_PREV_DAY_DATE: prevDate,
    EQUITY_BUFFER_V1: prevBuffer
  };

  // 3. Validate routing and forward config to library (library is property-free)
  globalThis.CURRENT_RUNTIME_CONFIG = cfg;
  Logger.log('[Bootstrap] runEquityRotator: forwarding cfg with assets=' + (cfg.assets ? cfg.assets.length : 0));
  assertRoutingConfig_(cfg);

  // 4. Call library for equity rotation; library returns DTO, host persists if present
  const result = callCoreFn_(['appendDailyEquityImpl', 'appendDailyEquity'], cfg);
  Logger.log('[Bootstrap] EquityRotator complete → ' + JSON.stringify(result));

  // 5. Host-side persist only: update DP if result provides newPrevDayEquity/Date/Buffer
  try {
    let didWrite = false;
    const dpUpdate = PropertiesService.getDocumentProperties();
    if (result && typeof result === 'object') {
      // newPrevDayEquity/Date are canonical for baseline
      if (typeof result.newPrevDayEquity !== 'undefined' && typeof result.newPrevDayDate !== 'undefined') {
        dpUpdate.setProperty('EQUITY_PREV_DAY_EUR', Number(result.newPrevDayEquity).toFixed(2));
        dpUpdate.setProperty('EQUITY_PREV_DAY_DATE', result.newPrevDayDate);
        didWrite = true;
        Logger.log(`[Bootstrap] Updated DP baseline → ${result.newPrevDayEquity} @ ${result.newPrevDayDate}`);
      }
      // equityBufferJson is optional rolling buffer (14 days)
      if (typeof result.equityBufferJson !== 'undefined') {
        dpUpdate.setProperty('EQUITY_BUFFER_V1', result.equityBufferJson);
        didWrite = true;
        // Debug log: show number of entries in equityBufferJson if parsable
        try {
          const buf = JSON.parse(result.equityBufferJson);
          let count = Array.isArray(buf) ? buf.length : (buf && typeof buf === 'object' ? Object.keys(buf).length : 0);
          Logger.log(`[Bootstrap] equityBufferJson entries: ${count}`);
        } catch (eBuf) {
          Logger.log('[Bootstrap] equityBufferJson not parsable: ' + (eBuf && eBuf.message ? eBuf.message : eBuf));
        }
      }
    }
    if (!didWrite) {
      Logger.log('[Bootstrap] No DP update performed (result DTO missing expected keys).');
    }
  } catch (eDP) {
    Logger.log('[Bootstrap] Failed to persist equity baseline: ' + (eDP && eDP.message ? eDP.message : eDP));
  }

  // 6. Invalidate NAV cache after any equity write
  try {
    if (typeof invalidateEquityNavDtoCache_ === 'function') {
      invalidateEquityNavDtoCache_();
    }
  } catch (_) {}

  // 7. No library or cross-layer DP writes allowed outside this host persist step
  return result;
}

/** Monthly archive job (trigger target). */
function runMonthlyArchive() {
  requireGlobalConfig_();
  assertNoScriptProperties_();
  var cfg = buildConfigForRun_();
  cfg = injectRuntimeCredsFromUP_(cfg);
  try {
    cfg = ensureLoggingDefaults_(cfg);
    try {
      cfg = RisontisCoreLibrary.ConfigUtils.validateConfigLite(cfg);
      Logger.log('[Bootstrap] Performed light config validation for non-trading task.');
    } catch (e) {
      Logger.log('[Bootstrap] validateConfigLite failed: ' + (e && e.message ? e.message : e));
      throw e;
    }
  } catch (e) {
    throw new Error('[Bootstrap] resolveSettings (library) failed early: ' + (e && e.message ? e.message : e));
  }
  globalThis.CURRENT_RUNTIME_CONFIG = cfg;
  Logger.log('[Bootstrap] runMonthlyArchive: forwarding cfg with assets=' + (cfg.assets ? cfg.assets.length : 0));
  assertRoutingConfig_(cfg);
  const result = callCoreFn_(['runMonthlyArchiveImpl','runMonthlyArchive'], cfg);
  try {
    runSheetStyling();
  } catch (e) {
    Logger.log('[Bootstrap] runSheetStyling after monthly archive failed: ' + (e && e.message ? e.message : e));
  }
  // Invalidate cached trades archive DTO after monthly archive writes
  try {
    if (typeof invalidateTradesArchiveDtoCache_ === 'function') {
      invalidateTradesArchiveDtoCache_();
    }
  } catch (_) {}

  // Close stale window immediately after successful monthly archive.
  var refreshResult = runArchiveCachesRefresh();
  if (!refreshResult || refreshResult.ok === false || !refreshResult.trades || refreshResult.trades.ok !== true) {
    var refreshErr = (refreshResult && refreshResult.error)
      ? refreshResult.error
      : 'unknown archive refresh failure';
    throw new Error('[Bootstrap] ARCHIVE_REFRESH_AFTER_MONTHLY_FAILED: ' + refreshErr);
  }

  return result;
}

/**
 * MonitorWriter flush (trigger target).
 */
function runMonitorWriterTrigger() {
  requireGlobalConfig_();
  assertNoScriptProperties_();
  var cfg = buildConfigForRun_();
  cfg = injectRuntimeCredsFromUP_(cfg);
  try {
    cfg = ensureLoggingDefaults_(cfg);
    try {
      cfg = RisontisCoreLibrary.ConfigUtils.validateConfigLite(cfg);
      Logger.log('[Bootstrap] Performed light config validation for non-trading task.');
    } catch (e) {
      Logger.log('[Bootstrap] validateConfigLite failed: ' + (e && e.message ? e.message : e));
      throw e;
    }
  } catch (e) {
    throw new Error('[Bootstrap] resolveSettings (library) failed early: ' + (e && e.message ? e.message : e));
  }
  globalThis.CURRENT_RUNTIME_CONFIG = cfg;
  Logger.log('[Bootstrap] runMonitorWriterTrigger: forwarding cfg with assets=' + (cfg.assets ? cfg.assets.length : 0));
  // No monitor snapshot or DP writes are performed in this trigger.
  return callCoreFn_(['runMonitorWriter'], cfg);
}

/**
 * Sheet styling entrypoint (trigger target).
 */
function runSheetStyling() {
  requireGlobalConfig_();
  assertNoScriptProperties_();

  var cfg = buildConfigForRun_();
  cfg = injectRuntimeCredsFromUP_(cfg);
  try {
    cfg = ensureLoggingDefaults_(cfg);
    try {
      cfg = RisontisCoreLibrary.ConfigUtils.validateConfigLite(cfg);
      Logger.log('[Bootstrap] Performed light config validation for non-trading task.');
    } catch (e) {
      Logger.log('[Bootstrap] validateConfigLite failed: ' + (e && e.message ? e.message : e));
      throw e;
    }
  } catch (e) {
    throw new Error('[Bootstrap] resolveSettings (library) failed in runSheetStyling: ' + (e && e.message ? e.message : e));
  }

  // Inject styling/settings cache state (host-owned DTOs)
  try {
    cfg.PersistedState = cfg.PersistedState || {};
    cfg.PersistedState.stylingState = hostGetStylingStateDto_();
  } catch (_) {}
  try {
    cfg.PersistedState = cfg.PersistedState || {};
    cfg.PersistedState.settingsCache = hostGetSettingsCacheDto_();
  } catch (_) {}
  globalThis.CURRENT_RUNTIME_CONFIG = cfg;

  try {
    try {
      callCoreFn_(['RunStylingPass']);
    } catch (_inner) {
      if (typeof globalThis.RunStylingPass === 'function') {
        globalThis.RunStylingPass();
      } else {
        Logger.log('[Styling] No styling entrypoint available in library or host; skipping.');
      }
    }
  } catch (e2) {
    throw new Error('[Bootstrap] runSheetStyling failed: ' + (e2 && e2.message ? e2.message : e2));
  }

  // Apply styling/settings state updates emitted by the library
  try {
    var updates = cfg && cfg.__stateUpdates;
    if (updates && updates.styling) {
      applyStylingStateUpdates_(updates.styling);
    }
    if (updates && updates.settingsCache) {
      applySettingsCacheUpdate_(updates.settingsCache);
    }
  } catch (eApply) {
    try { Logger.log('[Bootstrap] Failed to persist styling/settings updates: ' + (eApply && eApply.message ? eApply.message : eApply)); } catch(_) {}
  }
}

/**
 * Adds the Risontis menu for manual setup entry.
 * Setup is started only via the menu: Risontis → Run Setup.
 */
function onOpen(e) {
  try {
    var ui = SpreadsheetApp.getUi();
    ui.createMenu('Risontis')
      .addItem('Run Setup', 'hostRunSetupFromMenu')
      .addSeparator()
      .addItem('Check for Updates', 'hostCheckForUpdatesFromMenu')
      .addToUi();
  } catch (_) {}
}

// Removed: __isSetupRequested_, __clearSetupRequested_, __writeSetupStatusMeta_, hostConfirmSetupRequest, DP_KEY_SETUP_REQUESTED, menu-based setup.

/**
 * @DEPRECATED
 * Legacy setup entrypoint from pre-menu / sidebar-based installation flow.
 * This path is no longer used. Canonical setup is started via:
 *   Menu → Risontis → Run Setup → hostRunSetupFromMenu()
 *
 * Kept commented for reference only; safe to remove after full deprecation window.
 */
function hostRunSetup() {
  // LEGACY CODE COMMENTED OUT — DO NOT USE
  /*
  try {
    var ssToast = SpreadsheetApp.getActiveSpreadsheet();
    try {
      if (ssToast) {
        ssToast.toast('Installing Risontis… this may take a moment.', 'Risontis', -1);
      }
    } catch (_) {}

    setActiveSpreadsheetBindingIfMissing_();

    var cfg = (typeof GLOBAL_CONFIG !== 'undefined') ? GLOBAL_CONFIG : null;
    var result = null;

    if (globalThis.RisontisCoreLibrary &&
        RisontisCoreLibrary.SetupRunner &&
        typeof RisontisCoreLibrary.SetupRunner.ensureInstall === 'function') {
      result = RisontisCoreLibrary.SetupRunner.ensureInstall(cfg);
    } else if (typeof globalThis.SetupRunner !== 'undefined' &&
               typeof globalThis.SetupRunner.ensureInstall === 'function') {
      result = globalThis.SetupRunner.ensureInstall(cfg);
    }
    // @DEPRECATED legacy sidebar-based setup path — no longer supported
    // else if (typeof sidebarHandleRunSetup === 'function') {
    //   result = sidebarHandleRunSetup();
    // }
    else {
      throw new Error('SetupRunner.ensureInstall not available on host context.');
    }

    // Fallback trigger install (SetupRunner already ensures this, but keep legacy helper)
    try {
      if (typeof setupAllRequiredTriggers === 'function') {
        setupAllRequiredTriggers();
      }
    } catch (_trigErr) {
      // Non-fatal: triggers can be installed later via menu
      // Non-fatal: triggers can be installed later via menu/sidebar
    }


    // Optional initial run (safe-guarded)
    try {
      RunTradingSystem();
    } catch (_initErr) {
      // Non-fatal: user can run manually after adding keys
    }

    // Mark first-run complete in UserProperties (no SP allowed)
    try {
      PropertiesService.getUserProperties().setProperty('FIRST_RUN', '1');
      var ss = SpreadsheetApp.getActive();
      if (ss) {
        ss.toast('Setup completed. Reload this spreadsheet to open the Control Center.', 'Risontis', 8);
      }
    } catch (_) {}

    return result;
  } catch (e) {
    throw e;
  }
  */
}

/**
 * @DEPRECATED
 * Binding is handled inside SetupRunner.ensureInstall().
 * This helper is retained commented for historical context only.
 */
function setActiveSpreadsheetBindingIfMissing_() {
  // LEGACY — binding now handled by SetupRunner
  /*
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    if (!ss) return;
    var id = ss.getId();
    var up = PropertiesService.getUserProperties();
    var current = up.getProperty('ACTIVE_SPREADSHEET_ID');
    if (current !== id) {
      up.setProperty('ACTIVE_SPREADSHEET_ID', id);
    }
  } catch (_) {}
  */
}
// === Archive orchestration (Trades + Equity) ===
// Trades archive refresh (library builds, host persists)
globalThis.hostRefreshArchiveCache = function() {
  var cfg = buildArchiveRefreshConfig_();
  globalThis.CURRENT_RUNTIME_CONFIG = cfg;
  return RisontisCoreLibrary.ArchiveLoader.refreshArchiveCache(cfg);
};

// Equity archive refresh (library builds, host persists)

globalThis.hostRefreshEquityCache = function() {
  var cfg = buildArchiveRefreshConfig_();
  globalThis.CURRENT_RUNTIME_CONFIG = cfg;
  var res = RisontisCoreLibrary.ArchiveLoader.refreshEquityCache(cfg);
  try {
    if (typeof invalidateEquityNavDtoCache_ === 'function') {
      invalidateEquityNavDtoCache_();
    }
  } catch (_) {}
  return res;
};

function __resolveDpMaxBytesLimit__() {
  var limit = 8500;
  try {
    if (globalThis.GLOBAL_CONFIG && GLOBAL_CONFIG.Render && GLOBAL_CONFIG.Render.DP && Number(GLOBAL_CONFIG.Render.DP.MAX_BYTES)) {
      limit = Number(GLOBAL_CONFIG.Render.DP.MAX_BYTES);
    }
  } catch (_) {}
  return limit;
}

var ARCHIVE_TRADES_HOT_WINDOW_DAYS = 90;

function __resolveArchiveHotCutoffDate__(windowDays) {
  var days = Number(windowDays);
  if (!isFinite(days) || days < 1) days = ARCHIVE_TRADES_HOT_WINDOW_DAYS;
  days = Math.floor(days);
  var now = new Date();
  var cutoff = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0);
  cutoff.setDate(cutoff.getDate() - (days - 1));
  return cutoff;
}

function __parseArchiveTradeDateValue__(v) {
  if (v == null || v === '') return null;
  if (v instanceof Date) return isNaN(v.getTime()) ? null : v;
  var d = new Date(String(v));
  return isNaN(d.getTime()) ? null : d;
}

function __extractArchiveTradeEventDate__(row) {
  if (!row || typeof row !== 'object') return null;
  return __parseArchiveTradeDateValue__(row['Sell Date/Time'] || row['SELL_TS'] || row['SellTs'])
    || __parseArchiveTradeDateValue__(row['Buy Date/Time'] || row['BUY_TS'] || row['BuyTs']);
}

function __filterArchiveCacheByHotWindow__(cacheByYear, cutoffDate) {
  var out = {};
  var keptRows = 0;
  var droppedRows = 0;

  Object.keys(cacheByYear || {}).sort().forEach(function(yearStr) {
    var monthsObj = cacheByYear[yearStr];
    var filteredMonths = {};

    if (monthsObj && typeof monthsObj === 'object') {
      Object.keys(monthsObj).sort().forEach(function(ym) {
        var rows = Array.isArray(monthsObj[ym]) ? monthsObj[ym] : [];
        if (!rows.length) return;

        var kept = [];
        rows.forEach(function(row) {
          var eventDate = __extractArchiveTradeEventDate__(row);
          if (!(eventDate instanceof Date) || isNaN(eventDate.getTime()) || eventDate >= cutoffDate) {
            kept.push(row);
            keptRows++;
          } else {
            droppedRows++;
          }
        });

        if (kept.length) {
          filteredMonths[ym] = kept;
        }
      });
    }

    out[String(yearStr)] = filteredMonths;
  });

  return {
    cacheByYear: out,
    keptRows: keptRows,
    droppedRows: droppedRows
  };
}

function __splitArchiveMonthTradesForDp__(yearStr, ym, trades, limit) {
  if (!Array.isArray(trades) || !trades.length) return [];

  function buildFragment_(list) {
    return {
      version: '2.1',
      year: String(yearStr),
      ym: String(ym),
      trades: list
    };
  }

  function sizeBytes_(list) {
    return __utf8Bytes__(JSON.stringify(buildFragment_(list)));
  }

  var out = [];
  var buf = [];

  trades.forEach(function(row) {
    var candidate = buf.slice();
    candidate.push(row);
    if (sizeBytes_(candidate) <= limit) {
      buf = candidate;
      return;
    }

    if (!buf.length) {
      throw new Error('ARCHIVE_TRADES_STAGING_CHUNK_OVERSIZE_SINGLE_ROW: year=' + yearStr + ' ym=' + ym);
    }

    out.push(buf);
    buf = [row];

    if (sizeBytes_(buf) > limit) {
      throw new Error('ARCHIVE_TRADES_STAGING_CHUNK_OVERSIZE_SINGLE_ROW: year=' + yearStr + ' ym=' + ym);
    }
  });

  if (buf.length) out.push(buf);
  return out;
}

function __buildArchiveTradeChunksFromCacheByYear__(cacheByYear, limit) {
  var chunks = {};
  var manifests = {};

  Object.keys(cacheByYear || {}).sort().forEach(function(yearStr) {
    var monthsObj = cacheByYear[yearStr] || {};
    var partKeys = [];
    var bytes = [];
    var idx = 1;

    Object.keys(monthsObj).sort().forEach(function(ym) {
      var monthTrades = Array.isArray(monthsObj[ym]) ? monthsObj[ym] : [];
      var parts = __splitArchiveMonthTradesForDp__(yearStr, ym, monthTrades, limit);

      parts.forEach(function(partTrades) {
        var key = 'ARCHIVE_CACHE_TRADES_' + yearStr + '_P' + (idx++) + '_V2';
        var fragment = {
          version: '2.1',
          year: String(yearStr),
          ym: String(ym),
          trades: partTrades
        };
        var json = JSON.stringify(fragment);
        var size = __utf8Bytes__(json);
        if (size > limit) {
          throw new Error('ARCHIVE_TRADES_STAGING_CHUNK_OVERSIZE: key=' + key + ' bytes=' + size + ' limit=' + limit);
        }
        chunks[key] = json;
        partKeys.push(key);
        bytes.push(size);
      });
    });

    manifests[String(yearStr)] = {
      ver: '2.0',
      parts: partKeys,
      bytes: bytes,
      ts: new Date().toISOString()
    };
  });

  return { chunks: chunks, manifests: manifests };
}

function __buildArchiveTradesStagedPlan__(tradesResult, generationId) {
  if (!tradesResult || typeof tradesResult !== 'object') {
    throw new Error('ARCHIVE_TRADES_STAGING_INVALID_INPUT: missing trades result');
  }

  var sourceCacheByYear = (tradesResult.cacheByYear && typeof tradesResult.cacheByYear === 'object')
    ? tradesResult.cacheByYear
    : null;
  if (!sourceCacheByYear) {
    throw new Error('ARCHIVE_TRADES_STAGING_INVALID_INPUT: missing cacheByYear');
  }

  var limit = __resolveDpMaxBytesLimit__();
  var cutoffDate = __resolveArchiveHotCutoffDate__(ARCHIVE_TRADES_HOT_WINDOW_DAYS);
  var bounded = __filterArchiveCacheByHotWindow__(sourceCacheByYear, cutoffDate);
  var rebuilt = __buildArchiveTradeChunksFromCacheByYear__(bounded.cacheByYear, limit);

  var chunks = rebuilt.chunks;
  var manifests = rebuilt.manifests;
  var years = Object.keys(manifests).sort();
  var planByYear = {};
  var emptyYears = [];

  years.forEach(function(year) {
    var manifest = manifests[year];
    if (!manifest || !Array.isArray(manifest.parts)) {
      throw new Error('ARCHIVE_TRADES_STAGING_INVALID_MANIFEST: year=' + year);
    }

    var canonicalParts = manifest.parts.map(function(k) { return String(k || '').trim(); }).filter(Boolean);
    if (!canonicalParts.length) {
      emptyYears.push(String(year));
      return;
    }

    var stagedPartEntries = [];
    var stagedPartKeys = [];

    canonicalParts.forEach(function(canonicalKey, idx) {
      var payload = chunks[canonicalKey];
      if (typeof payload !== 'string') {
        throw new Error('ARCHIVE_TRADES_STAGING_MISSING_CHUNK: key=' + canonicalKey + ' year=' + year);
      }

      var payloadBytes = __utf8Bytes__(payload);
      if (payloadBytes > limit) {
        throw new Error('ARCHIVE_TRADES_STAGING_CHUNK_OVERSIZE: key=' + canonicalKey + ' bytes=' + payloadBytes + ' limit=' + limit);
      }

      var stageKey = 'ARCHIVE_CACHE_TRADES_STAGE_' + generationId + '_' + year + '_P' + (idx + 1) + '_V2';
      stagedPartEntries.push({
        canonicalKey: canonicalKey,
        stageKey: stageKey,
        payload: payload
      });
      stagedPartKeys.push(stageKey);
    });

    var stagedManifest = {};
    Object.keys(manifest).forEach(function(k) { stagedManifest[k] = manifest[k]; });
    stagedManifest.parts = stagedPartKeys.slice();

    var manifestRaw = JSON.stringify(stagedManifest);
    var manifestBytes = __utf8Bytes__(manifestRaw);
    if (manifestBytes > limit) {
      throw new Error('ARCHIVE_TRADES_STAGING_MANIFEST_OVERSIZE: year=' + year + ' bytes=' + manifestBytes + ' limit=' + limit);
    }

    planByYear[String(year)] = {
      year: String(year),
      manifestKey: 'ARCHIVE_CACHE_TRADES_' + year + '_MANIFEST_V1',
      stagedManifestRaw: manifestRaw,
      stagedPartEntries: stagedPartEntries
    };
  });

  return {
    generationId: generationId,
    planByYear: planByYear,
    yearsWithData: Object.keys(planByYear).sort(),
    emptyYears: emptyYears,
    hotWindowDays: ARCHIVE_TRADES_HOT_WINDOW_DAYS,
    hotCutoffIso: cutoffDate.toISOString(),
    boundedRowsKept: bounded.keptRows,
    boundedRowsDropped: bounded.droppedRows
  };
}

function __applyArchiveTradesStagedCommit__(dp, tradesResult) {
  if (!dp || typeof dp.setProperty !== 'function' || typeof dp.getProperties !== 'function') {
    throw new Error('ARCHIVE_TRADES_STAGING_DP_UNAVAILABLE');
  }

  var existingProps = dp.getProperties() || {};
  var existingByYear = {};

  Object.keys(existingProps).forEach(function(key) {
    var m = key.match(TRADES_MANIFEST_REGEX);
    if (!m) return;
    var year = String(m[1]);
    var manifestRaw = existingProps[key];
    var manifest = null;
    try { manifest = JSON.parse(manifestRaw); } catch (_) { manifest = null; }
    var parts = (manifest && Array.isArray(manifest.parts))
      ? manifest.parts.map(function(p) { return String(p || '').trim(); }).filter(Boolean)
      : [];
    existingByYear[year] = {
      manifestKey: key,
      partKeys: parts
    };
  });

  var generationId = String(Date.now()) + '_' + Math.floor(Math.random() * 1000000);
  var staged = __buildArchiveTradesStagedPlan__(tradesResult, generationId);
  var writtenStageKeys = [];
  var committedYears = [];

  try {
    staged.yearsWithData.forEach(function(year) {
      var row = staged.planByYear[year];
      row.stagedPartEntries.forEach(function(entry) {
        dp.setProperty(entry.stageKey, entry.payload);
        writtenStageKeys.push(entry.stageKey);
      });
    });
  } catch (eStageWrite) {
    writtenStageKeys.forEach(function(stageKey) {
      try { dp.deleteProperty(stageKey); } catch (_) {}
    });
    throw new Error('ARCHIVE_TRADES_STAGE_WRITE_FAILED: ' + (eStageWrite && eStageWrite.message ? eStageWrite.message : eStageWrite));
  }

  try {
    staged.yearsWithData.forEach(function(year) {
      var row = staged.planByYear[year];
      dp.setProperty(row.manifestKey, row.stagedManifestRaw);
      committedYears.push(year);
    });
  } catch (eCommit) {
    throw new Error('ARCHIVE_TRADES_MANIFEST_COMMIT_FAILED: ' + (eCommit && eCommit.message ? eCommit.message : eCommit));
  }

  committedYears.forEach(function(year) {
    var before = existingByYear[year];
    var after = staged.planByYear[year];
    if (!after) return;
    var live = {};
    after.stagedPartEntries.forEach(function(entry) {
      live[entry.stageKey] = true;
    });

    if (before && Array.isArray(before.partKeys)) {
      before.partKeys.forEach(function(oldKey) {
        if (!live[oldKey]) {
          try { dp.deleteProperty(oldKey); } catch (_) {}
        }
      });
    }
  });

  staged.emptyYears.forEach(function(year) {
    var before = existingByYear[year];
    if (!before) return;

    if (Array.isArray(before.partKeys)) {
      before.partKeys.forEach(function(oldKey) {
        dp.deleteProperty(oldKey);
      });
    }
    if (before.manifestKey) {
      dp.deleteProperty(before.manifestKey);
    }
  });

  return {
    ok: true,
    generationId: generationId,
    committedYears: committedYears,
    emptyYears: staged.emptyYears,
    stagedYears: staged.yearsWithData,
    hotWindowDays: staged.hotWindowDays,
    hotCutoffIso: staged.hotCutoffIso,
    boundedRowsKept: staged.boundedRowsKept,
    boundedRowsDropped: staged.boundedRowsDropped
  };
}

/**
 * Combined orchestrator for archive cache refresh (Trades + Equity) that delegates all logic to the library.
 */
function runArchiveCachesRefresh() {
  requireGlobalConfig_();
  assertNoScriptProperties_();

  const dp = PropertiesService.getDocumentProperties();
  try { Logger.log('[Bootstrap] runArchiveCachesRefresh: delegating to library…'); } catch(_){ }

  // 1) Get DTO from library
  var cfg = buildArchiveRefreshConfig_();
  globalThis.CURRENT_RUNTIME_CONFIG = cfg;
  const result = RisontisCoreLibrary.ArchiveLoader.refreshArchiveCaches(cfg);
  if (!result || result.ok === false) {
    try { Logger.log('[Bootstrap] runArchiveCachesRefresh aborted: refreshArchiveCaches failed.'); } catch(_){ }
    return result;
  }
  if (!result.trades || result.trades.ok !== true) {
    try { Logger.log('[Bootstrap] runArchiveCachesRefresh aborted: trades refresh failed.'); } catch(_){ }
    return result;
  }

  // 2) Stage + commit (manifest-last) for trade cache, no purge-first.
  try {
    var stagedCommit = __applyArchiveTradesStagedCommit__(dp, result.trades);
    try {
      Logger.log('[Bootstrap] runArchiveCachesRefresh trades commit ok: years=' + stagedCommit.committedYears.join(',') + ' emptyYears=' + stagedCommit.emptyYears.join(',') + ' hotDays=' + stagedCommit.hotWindowDays + ' hotCutoff=' + stagedCommit.hotCutoffIso + ' kept=' + stagedCommit.boundedRowsKept + ' dropped=' + stagedCommit.boundedRowsDropped);
    } catch (_) {}
  } catch (eCommitTrades) {
    var msg = (eCommitTrades && eCommitTrades.message) ? eCommitTrades.message : String(eCommitTrades || 'unknown');
    try { Logger.log('[Bootstrap] runArchiveCachesRefresh failed: ' + msg); } catch(_){ }
    return { ok: false, error: msg };
  }

  // 3) Write equity cache (per year) — unified object schema (timestamp + data[])
  if (result && result.equity && result.equity.cacheByYear) {
    const eq = result.equity.cacheByYear; // { year: arr }
    Object.keys(eq).forEach(function(year){
      try {
        const obj = {
          timestamp: Date.now(),
          data: eq[year]  // normalized array of { date, dailyPnl, equity }
        };
        dp.setProperty(
          'ARCHIVE_CACHE_EQUITY_' + year,
          JSON.stringify(obj)
        );
      } catch(_) {}
    });
  }

  // 4) Invalidate cached archive DTOs so next callers rebuild from fresh DP
  try {
    if (typeof invalidateTradesArchiveDtoCache_ === 'function') {
      invalidateTradesArchiveDtoCache_();
    }
    if (typeof invalidateEquityNavDtoCache_ === 'function') {
      invalidateEquityNavDtoCache_();
    }
  } catch (_) {}

  try { Logger.log('[Bootstrap] runArchiveCachesRefresh: completed.'); } catch(_){ }
  return result;
}
