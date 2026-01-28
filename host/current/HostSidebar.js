/****
 * File: HostSidebar.js
 * Description: Host Control Center bridge (UserProperties guard, delegated UI render).
 * Version: 2.0.0
 * Date: 2025-12-10
 */

const _UP_KEYS = Object.freeze({
  BITVAVO_API_KEY: 'BITVAVO_API_KEY',
  BITVAVO_API_SECRET: 'BITVAVO_API_SECRET',
  ALERT_EMAIL: 'ALERT_EMAIL',
  ACTIVE_SPREADSHEET_ID: 'ACTIVE_SPREADSHEET_ID',
  TARGET_SHEET_ID: 'RISONTIS_TARGET_SHEET_ID'
});

const _HANDLERS = Object.freeze([
  'RunTradingSystem',
  'runSheetStyling',
  'rotateSystemLogSheet',
  'runMonthlyArchive',
  'runMonitorWriterTrigger'
]);

// Backend base URL for license service calls
const LICENSE_BACKEND_BASE = 'https://risontis-license-service-1028361675066.europe-west1.run.app';
// Public key (PEM) for license signature verification (safe to embed)
const LICENSE_PUBLIC_KEY = [
  '-----BEGIN PUBLIC KEY-----',
  'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4ZK9+w9GuwlUw5Oa66Ce',
  'sbkYuRybr7plCYg/efSppeO8HJ6dra482w1I1Yav9/9RSenBiQPRcf5RBFL2QCOM',
  'NK/N44zL/1idi05f/egELqs0WzRGeb0O1P/rW9aZ0mP9lxbndPpY3x8StmX4njnB',
  'Onv2C8Vc1hZpIQpcv/P2Wnb86LG+DxlYalOIA2hR7fXptQVoMzRKVZUvjwKcX23L',
  'cELe0GzgSgGQE4NhLtR3OMLSs31VEX7oTp2it8hU/1CcNy3A1+w8S9H3IWKvZCB8',
  'vM7rCXBQrsY2G8GBlqJEugxWuSzs8yIp7z1H+jHgi74ZAg/rViHZPWsz7oZdiHd0',
  'nwIDAQAB',
  '-----END PUBLIC KEY-----'
].join('\n');
const LICENSE_VERIFY_CACHE_MS = 24 * 60 * 60 * 1000; // 24h cache
const HOST_TRADES_MANIFEST_REGEX = /^ARCHIVE_CACHE_TRADES_(\d{4})_MANIFEST_V1$/;
var __hostTradesArchiveDtoCache = null;
function resolveSidebarTimezone_() {
  try {
    if (globalThis.RisontisCoreLibrary &&
        RisontisCoreLibrary.SettingsResolver &&
        typeof RisontisCoreLibrary.SettingsResolver.readSettings === 'function') {
      var kv = RisontisCoreLibrary.SettingsResolver.readSettings();
      if (kv && kv.TIMEZONE) {
        var tz = String(kv.TIMEZONE).trim();
        if (tz) return tz;
      }
    }
  } catch (_) {}
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var tz = ss ? ss.getSpreadsheetTimeZone() : null;
    if (tz) return tz;
  } catch (_) {}
  try {
    var stz = Session.getScriptTimeZone();
    if (stz) return stz;
  } catch (_) {}
  return 'Etc/UTC';
}

function getHostTradesArchiveDto_() {
  if (__hostTradesArchiveDtoCache) return __hostTradesArchiveDtoCache;
  // Prefer global builder from Bootstrap if present
  if (typeof globalThis.hostGetTradesArchiveDto_ === 'function') {
    try {
      __hostTradesArchiveDtoCache = globalThis.hostGetTradesArchiveDto_();
      if (__hostTradesArchiveDtoCache) return __hostTradesArchiveDtoCache;
    } catch (_) { /* fall through */ }
  }
  var dp = PropertiesService.getDocumentProperties();
  var props = dp.getProperties();
  if (typeof globalThis.buildTradesArchiveDtoFromProps_ === 'function') {
    __hostTradesArchiveDtoCache = globalThis.buildTradesArchiveDtoFromProps_(props);
  } else {
    throw new Error('[HostSidebar] Trades archive builder missing (Bootstrap not loaded).');
  }
  return __hostTradesArchiveDtoCache;
}
function invalidateHostTradesArchiveDtoCache_() {
  __hostTradesArchiveDtoCache = null;
  try { if (typeof globalThis.invalidateTradesArchiveDtoCache_ === 'function') { globalThis.invalidateTradesArchiveDtoCache_(); } } catch (_){}
}

// Debug logger (server-side); avoids throw if Logger is unavailable
function __ccLog_(tag, msg){
  try { Logger.log('[CC]['+ tag +'] ' + String(msg||'')); } catch (_) {}
}

function ensureActiveSpreadsheetBinding_() {
  try {
    var up = PropertiesService.getUserProperties();
    var cur = up.getProperty(_UP_KEYS.ACTIVE_SPREADSHEET_ID);
    if (!cur) {
      var ss = SpreadsheetApp.getActiveSpreadsheet();
      if (ss) up.setProperty(_UP_KEYS.ACTIVE_SPREADSHEET_ID, ss.getId());
    }
  } catch (_) {}
}

function ensureAppFolderBinding_() {
  var dp = PropertiesService.getDocumentProperties();
  var existing = (dp.getProperty('RISONTIS_APP_FOLDER_ID') || '').trim();
  if (existing) return { ok: true, id: existing, set: false };

  var lib = globalThis.RisontisCoreLibrary;
  var routing = (lib && lib.DriveRouting) ? lib.DriveRouting : (globalThis.DriveRouting || null);
  if (!routing || typeof routing.getOrCreateRootForSetup_ !== 'function') {
    throw new Error('[Setup] DriveRouting.getOrCreateRootForSetup_ unavailable.');
  }
  var folder = routing.getOrCreateRootForSetup_(
    (typeof GLOBAL_CONFIG !== 'undefined' ? GLOBAL_CONFIG : null)
  );
  if (!folder || typeof folder.getId !== 'function') {
    throw new Error('[Setup] App folder binding failed.');
  }
  var folderId = folder.getId();
  dp.setProperty('RISONTIS_APP_FOLDER_ID', folderId);
  return { ok: true, id: folderId, set: true };
}

function materializeSetupFolders_(config) {
  var cfg = (config && typeof config === 'object') ? Object.assign({}, config) : {};
  try {
    var bound = PropertiesService.getDocumentProperties().getProperty('RISONTIS_APP_FOLDER_ID');
    if (bound && !cfg.RISONTIS_APP_FOLDER_ID) {
      cfg.RISONTIS_APP_FOLDER_ID = bound;
    }
  } catch (_) {}
  var lib = globalThis.RisontisCoreLibrary;
  var routing = (lib && lib.DriveRouting) ? lib.DriveRouting : (globalThis.DriveRouting || null);
  if (!routing || typeof routing.resolveFolder_ !== 'function') {
    throw new Error('[Setup] DriveRouting.resolveFolder_ unavailable.');
  }
  var constants = (lib && lib.Constants) ? lib.Constants : (globalThis.Constants || null);
  var folders = constants && constants.STORAGE_ROUTING_DEFAULTS && constants.STORAGE_ROUTING_DEFAULTS.Folders;
  if (!folders || typeof folders !== 'object') {
    throw new Error('[Setup] StorageRouting defaults missing.');
  }
  Object.keys(folders).forEach(function(key) {
    if (key === 'License') return;
    routing.resolveFolder_(key, cfg);
  });
}

// Guard to ensure setup only runs in the intended target sheet (per-user binding stored in UP).
function ensureActiveSheetMatchesTarget_() {
  var up = PropertiesService.getUserProperties();
  var target = (up.getProperty(_UP_KEYS.TARGET_SHEET_ID) || '').trim();
  if (!target) return { ok: true, targetId: null, activeId: null };
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var activeId = ss ? ss.getId() : null;
  if (activeId && target && activeId !== target) {
    throw new Error('[Setup] Active sheet does not match your Risontis target. Open your copied sheet and retry.');
  }
  return { ok: true, targetId: target, activeId: activeId };
}

function openControlCenter() {
  // Route to fat-library provider to avoid host legacy UI
  return openLibraryControlCenter();
}

function openLibraryControlCenter() {
  try {
    // Touch only OAuth-free scope to keep simple onOpen compatible
    try { SpreadsheetApp.getActiveSpreadsheet().getName(); } catch (_) {}

    // Harden: check for fat-library provider only, with runtime logging
    var hasNS = !!globalThis.RisontisCoreLibrary;
    var hasProv = hasNS && (typeof globalThis.RisontisCoreLibrary.getControlCenterSidebarHtmlString === 'function');
    try { Logger.log('[CCUI] ns=' + hasNS + ' provider=' + hasProv + ' ver=' + __readLibraryVersion_()); } catch(_) {}

    var htmlContent = null;
    if (hasProv) {
      try {
        htmlContent = String(globalThis.RisontisCoreLibrary.getControlCenterSidebarHtmlString());
        try { Logger.log('[CCUI][provider OK] len=' + (htmlContent ? htmlContent.length : 0) + ' head=' + (htmlContent ? htmlContent.substring(0, 80) : 'null')); } catch(_) {}
      } catch (eProv) {
        try { Logger.log('[CCUI][provider EXC] ' + (eProv && eProv.message ? eProv.message : eProv)); } catch(_) {}
        htmlContent = null;
      }
    }

    if (!htmlContent) {
      var msg = '<div style="font:13px Arial;padding:12px">'
        + '<b>Control Center UI unavailable</b><br>'
        + 'Missing fat-library provider: <code>RisontisCoreLibrary.getControlCenterSidebarHtmlString()</code>.'
        + '<br>Update the library in the host to the latest version and try again.</div>';
      var outErr = HtmlService.createHtmlOutput(msg)
        .setTitle('\u200B');
      SpreadsheetApp.getUi().showSidebar(outErr);
      return { ok:false, message:'Missing CCUI provider in library' };
    }

    // HOST‑served HtmlOutput ensures google.script.run binds to host project (RPCs hit host functions)
    var hostOut = HtmlService
      .createHtmlOutput(htmlContent)
      .setTitle('\u200B');
    // Cache-buster to prevent stale HTML in Apps Script sandbox
    hostOut.append('<!-- ' + Date.now() + ' -->');

    SpreadsheetApp.getUi().showSidebar(hostOut);
    return { ok: true, message: 'Opened host‑served Control Center.' };
  } catch (e) {
    try { Logger.log('[HostSidebar#openLibraryControlCenter] ' + (e && e.message ? e.message : e)); } catch(_){ }
    return { ok: false, message: (e && e.message ? e.message : String(e||'unknown')) };
  }
}

// ================= Server-side helpers (no secrets in logs) =================

function __readLibraryVersion_() {
  try {
    var lib = globalThis.RisontisCoreLibrary;
    if (!lib) return '(unloaded)';
    if (lib.Constants && lib.Constants.BUILD && lib.Constants.BUILD.LIB_VERSION) return String(lib.Constants.BUILD.LIB_VERSION);
    if (lib.VERSION) return String(lib.VERSION);
    if (lib.Constants && lib.Constants.VERSION) return String(lib.Constants.VERSION);
    return '(unknown)';
  } catch (_) { return '(unknown)'; }
}

function ping_() {
  try { return { ok: true, ts: Date.now() }; } catch (_) { return { ok: true }; }
}

function primePermissions_() {
  __ccLog_('prime','invoked');
  try {
    var trigCount = 0;
    try { trigCount = (ScriptApp.getProjectTriggers() || []).length; } catch (_) {}
    var up = PropertiesService.getUserProperties();
    up.setProperty('RISONTIS_UP_PRIME', String(Date.now()));
    var okWrite = !!up.getProperty('RISONTIS_UP_PRIME');
    __ccLog_('prime', 'ok; triggers='+trigCount+', upWrite='+okWrite);
    return { ok: true, message: 'Prime OK (triggers=' + trigCount + ', upWrite=' + okWrite + ')' };
  } catch (e) {
    __ccLog_('prime/ERR', (e && e.message ? e.message : e));
    return { ok: false, message: (e && e.message ? e.message : String(e||'unknown')) };
  }
}

function ccDispatch_(action, payload) {
  try {
    if (action === 'status') {
      return getRuntimeStatus_();
    }
    if (action === 'prime') {
      return primePermissions_();
    }
    if (action === 'save') {
      return sidebarSaveSecrets(payload || {});
    }
    return { ok:false, error:'Unknown action: ' + action };
  } catch (e) {
    __ccLog_('dispatch/ERR', (e && e.message ? e.message : e));
    return { ok:false, error:(e && e.message ? e.message : String(e||'unknown')) };
  }
}

// Public RPC wrappers (no underscore) for reliable google.script.run binding
function ccDispatch(action, payload) { return ccDispatch_(action, payload); }
function getRuntimeStatus() { return getRuntimeStatus_(); }
function primePermissions() { return primePermissions_(); }
function getEffectiveUserEmail() { return getEffectiveUserEmail_(); }
function hostSyncLicense() { return hostSyncLicense_(); }
function hostUpdateLicense(priceId) { return hostUpdateLicense_(priceId); }
function hostCreatePortalSession() { return hostCreatePortalSession_(); }

function setTradingEnabled(enabled) {
  try {
    // Prefer library implementation (single source of truth)
    if (globalThis.RisontisCoreLibrary && typeof RisontisCoreLibrary.setTradingEnabled === 'function') {
      return RisontisCoreLibrary.setTradingEnabled(enabled);
    }

    // Fallback: update Settings sheet label 'Trading Enabled' and mirror DP flag
    var on = (enabled === true || String(enabled).toLowerCase() === 'true' || String(enabled) === '1');

    var ss = SpreadsheetApp.getActive();
    var sh = ss && ss.getSheetByName('Settings');
    if (!sh) throw new Error('Settings sheet not found');
    var vals = sh.getDataRange().getValues();
    var row = -1;
    for (var r = 0; r < vals.length; r++) {
      if (String(vals[r][0]).trim() === 'Trading Enabled') { row = r + 1; break; }
    }
    if (row < 0) throw new Error('Row "Trading Enabled" not found in column A');
    sh.getRange(row, 2).setValue(on); // checkbox TRUE/FALSE

    var dp = PropertiesService.getDocumentProperties();
    dp.setProperty('TRADING_ENABLED', on ? '1' : '0');

    try { Logger.log('[HostSidebar#setTradingEnabled/FALLBACK] %s', on ? 'ON' : 'OFF'); } catch(_){}
    return { ok: true, tradingEnabled: on };
  } catch (e) {
    try { Logger.log('[HostSidebar#setTradingEnabled/ERR] ' + (e && e.message ? e.message : e)); } catch(_){}
    return { ok: false, error: (e && e.message) ? e.message : String(e||'unknown') };
  }
}

(function exposeLicenseVerifier_(){
  try {
    globalThis.LicenseVerifier = {
      verify: function(dto) {
        if (!dto || !dto.signature) return false;
        try {
          var hash = '';
          try { hash = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, JSON.stringify(dto)).map(function(b){return ('0' + (b & 0xFF).toString(16)).slice(-2);}).join(''); } catch(_){}
          var dp = PropertiesService.getDocumentProperties();
          var cachedHash = dp.getProperty('LICENSE_HASH') || '';
          var cachedAt = Number(dp.getProperty('LICENSE_VERIFIED_AT') || 0);
          var now = Date.now();
          if (hash && cachedHash === hash && (now - cachedAt) < LICENSE_VERIFY_CACHE_MS) {
            return true;
          }
          // Call backend to verify signature
          var resp = UrlFetchApp.fetch(LICENSE_BACKEND_BASE + '/verifyLicense', {
            method: 'post',
            contentType: 'application/json',
            payload: JSON.stringify({ license: dto }),
            muteHttpExceptions: true
          });
          var code = resp.getResponseCode();
          if (code !== 200) return false;
          var out = {};
          try { out = JSON.parse(resp.getContentText() || '{}'); } catch(_) { out = {}; }
          var ok = !!out.ok;
          if (ok && hash) {
            dp.setProperty('LICENSE_HASH', hash);
            dp.setProperty('LICENSE_VERIFIED_AT', String(now));
          }
          return ok;
        } catch (_) { return false; }
      }
    };
  } catch (_) {
    // fail-closed: no verifier
  }
})();

function hostSyncLicense_() {
  try {
    var email = getEffectiveUserEmail_();
    var sheet = SpreadsheetApp.getActiveSpreadsheet();
    if (!email) throw new Error('Unable to determine user email');
    if (!sheet) throw new Error('No active spreadsheet bound');

    var url = LICENSE_BACKEND_BASE + '/licenseSync'
      + '?email=' + encodeURIComponent(email)
      + '&sheetId=' + encodeURIComponent(sheet.getId());
    var resp = UrlFetchApp.fetch(url, {
      method: 'get',
      muteHttpExceptions: true
    });
    if (resp.getResponseCode() !== 200) {
      throw new Error('licenseSync failed: HTTP ' + resp.getResponseCode());
    }

    var body = {};
    try { body = JSON.parse(resp.getContentText() || '{}'); } catch (_) { body = {}; }

    var license = body.license || (body.type ? body : null);
    var proof = license ? buildLicenseProof_(license) : null;
    if (license && proof && proof.signatureOk) {
      var dp = PropertiesService.getDocumentProperties();
      dp.setProperty('LICENSE_JSON', JSON.stringify(license));
      return {
        ok: true,
        license: license,
        type: license.type,
        monthlyFee: license.monthlyFee,
        capitalBracket: license.capitalBracket,
        validUntil: license.validUntil,
        validUntilFormatted: formatLicenseValidUntil_(license),
        stage: 'active'
      };
    }

    return {
      ok: false,
      stage: body.stage || 'invalid',
      issues: body.issues || ['Signature verification failed']
    };
  } catch (e) {
    return { ok:false, stage:'error', issues:[(e && e.message) ? e.message : String(e || 'unknown')] };
  }
}

function verifyLicensePayload_(license) {
  if (!license || typeof license !== 'object') return false;
  try {
    if (globalThis.LicenseVerifier && typeof LicenseVerifier.verify === 'function') {
      return !!LicenseVerifier.verify(license);
    }
  } catch (_) {}
  return false;
}

function buildLicenseBinding_() {
  var binding = {};
  try {
    var email = getEffectiveUserEmail_();
    if (email) binding.email = email;
  } catch (_) {}
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    if (ss) binding.sheetId = ss.getId();
  } catch (_) {}
  return binding;
}

function buildLicenseProof_(licenseDto) {
  var proof = { signatureOk: false, binding: null };
  try { proof.signatureOk = verifyLicensePayload_(licenseDto); } catch (_) { proof.signatureOk = false; }
  try { proof.binding = buildLicenseBinding_(); } catch (_) { proof.binding = null; }
  return proof;
}

function readLicenseBackoffState_() {
  var state = {};
  try {
    var dp = PropertiesService.getDocumentProperties();
    var all = dp.getProperties();
    if (all && all.LICENSE_BACKOFF_COUNT) {
      state.LICENSE_BACKOFF_COUNT = all.LICENSE_BACKOFF_COUNT;
    }
    if (all && all.LICENSE_RETRY_AFTER) {
      state.LICENSE_RETRY_AFTER = all.LICENSE_RETRY_AFTER;
    }
  } catch (_) {}
  return state;
}

function resolveLicenseStatusViaLibrary_() {
  var dp = PropertiesService.getDocumentProperties();
  var raw = dp.getProperty('LICENSE_JSON') || '';
  if (!raw) {
    return { ok: false, stage: 'missing', issues: ['License JSON missing.'] };
  }

  var parsed = null;
  try {
    parsed = JSON.parse(raw);
  } catch (_) {
    return { ok: false, stage: 'invalid', issues: ['License JSON malformed.'] };
  }

  if (!globalThis.RisontisCoreLibrary ||
      !RisontisCoreLibrary.LicenseService ||
      typeof RisontisCoreLibrary.LicenseService.getStatus !== 'function') {
    return {
      ok: true,
      fallback: true,
      licensed: true,
      type: parsed.type || null,
      validUntil: parsed.validUntil || null,
      capitalBracket: (parsed.capitalBracket != null ? Number(parsed.capitalBracket) : null),
      stage: 'fallback'
    };
  }

  var proof = buildLicenseProof_(parsed);
  return RisontisCoreLibrary.LicenseService.getStatus({
    licenseJson: raw,
    signatureOk: proof.signatureOk,
    binding: proof.binding
  });
}

function formatLicenseValidUntil_(license) {
  try {
    if (!license || !license.validUntil) return '';
    var cfg = {};
    try {
      if (
        globalThis.RisontisCoreLibrary &&
        RisontisCoreLibrary.SettingsResolver &&
        typeof RisontisCoreLibrary.SettingsResolver.resolve === 'function'
      ) {
        cfg = RisontisCoreLibrary.SettingsResolver.resolve({}) || {};
      }
    } catch (_) { cfg = {}; }
    if (
      globalThis.RisontisCoreLibrary &&
      RisontisCoreLibrary.DateUtils &&
      typeof RisontisCoreLibrary.DateUtils.formatDateTimeWithSeconds === 'function'
    ) {
      return RisontisCoreLibrary.DateUtils.formatDateTimeWithSeconds(new Date(license.validUntil), cfg);
    }
    var tz = SpreadsheetApp.getActiveSpreadsheet()
      ? SpreadsheetApp.getActiveSpreadsheet().getSpreadsheetTimeZone()
      : (Session.getScriptTimeZone() || 'Etc/UTC');
    return Utilities.formatDate(new Date(license.validUntil), tz, 'dd/MM/yyyy HH:mm:ss');
  } catch (_) {
    return license && license.validUntil ? license.validUntil : '';
  }
}

function hostEnsureVerifiedLicense() {
  try {
    var dp = PropertiesService.getDocumentProperties();
    var raw = dp.getProperty('LICENSE_JSON');
    if (!raw) {
      return { ok: false, issues: ['License invalid or expired'] };
    }
    var cached = null;
    try {
      cached = JSON.parse(raw);
    } catch (_) {
      return { ok: false, issues: ['License invalid or expired'] };
    }
    var proof = buildLicenseProof_(cached);
    if (!globalThis.RisontisCoreLibrary ||
        !RisontisCoreLibrary.LicenseService ||
        typeof RisontisCoreLibrary.LicenseService.getStatus !== 'function') {
      return { ok: false, issues: ['License invalid or expired'] };
    }
    var status = RisontisCoreLibrary.LicenseService.getStatus({
      licenseJson: raw,
      signatureOk: proof.signatureOk,
      binding: proof.binding,
      requireHostProof: true
    });
    if (status && status.ok === true && status.stage === 'active') {
      return { ok: true, license: cached };
    }
    return { ok: false, issues: ['License invalid or expired'] };
  } catch (e) {
    return { ok:false, issues:[(e && e.message) ? e.message : String(e || 'unknown')] };
  }
}

globalThis.hostEnsureVerifiedLicense = hostEnsureVerifiedLicense;

function hostUpdateLicense_(priceId) {
  try {
    var email = Session.getActiveUser().getEmail();
    var sheetId = SpreadsheetApp.getActiveSpreadsheet().getId();
    var url = LICENSE_BACKEND_BASE + '/updateSubscription?email=' + encodeURIComponent(email) + '&sheetId=' + encodeURIComponent(sheetId) + '&priceId=' + encodeURIComponent(priceId);
    var resp = UrlFetchApp.fetch(url, {
      method: 'post',
      muteHttpExceptions: true
    });
    var code = resp.getResponseCode();
    if (code !== 200) return { ok:false, error:'updateSubscription failed: HTTP '+code };
    return JSON.parse(resp.getContentText() || '{}');
  } catch (e) {
    return { ok:false, error:(e && e.message)?e.message:String(e||'unknown') };
  }
}

function hostCreatePortalSession_() {
  try {
    var email = Session.getActiveUser().getEmail();
    var sheetId = SpreadsheetApp.getActiveSpreadsheet().getId();
    var url = LICENSE_BACKEND_BASE + '/createPortalSession?email=' + encodeURIComponent(email) + '&sheetId=' + encodeURIComponent(sheetId);
    var resp = UrlFetchApp.fetch(url, {
      method: 'post',
      muteHttpExceptions: true
    });
    var code = resp.getResponseCode();
    if (code !== 200) return { ok:false, error:'createPortalSession failed: HTTP '+code };
    return JSON.parse(resp.getContentText() || '{}');
  } catch (e) {
    return { ok:false, error:(e && e.message)?e.message:String(e||'unknown') };
  }
}

function getControlCenterStatus() {
  try {
    if (globalThis.RisontisCoreLibrary && typeof RisontisCoreLibrary.getControlCenterStatus === 'function') {
      return RisontisCoreLibrary.getControlCenterStatus();
    }
    throw new Error('Library getControlCenterStatus not available');
  } catch (e) {
    try { Logger.log('[HostSidebar#getControlCenterStatus] ' + (e && e.message ? e.message : e)); } catch(_) {}
    return { ok:false, error: (e && e.message) ? e.message : String(e||'unknown') };
  }
}

function hostGetLicenseStatus() {
  try {
    var status = resolveLicenseStatusViaLibrary_();
    if (!status) {
      return { ok:false, licensed:false, error:'License status unavailable' };
    }
    if (status.stage === 'missing') {
      return { ok: true, licensed: false };
    }
    if (status.ok) {
      var vu = status.validUntil || null;
      return {
        ok: true,
        licensed: true,
        type: status.type || null,
        validUntil: vu,
        monthlyFee: (status.monthlyFee != null ? Number(status.monthlyFee) : null),
        capitalBracket: (status.capitalBracket != null ? Number(status.capitalBracket) : null),
        validUntilFormatted: formatLicenseValidUntil_({ validUntil: vu })
      };
    }
    return {
      ok: false,
      licensed: false,
      stage: status.stage || 'invalid',
      issues: status.issues || []
    };
  } catch (e) {
    try { Logger.log('[HostSidebar#hostGetLicenseStatus ERR] ' + (e && e.message ? e.message : e)); } catch(_){ }
    return { ok:false, licensed:false, error: (e && e.message ? e.message : String(e||'unknown')) };
  }
}
globalThis.hostGetLicenseStatus = hostGetLicenseStatus;
globalThis.hostBuildTradesDto = hostBuildTradesDto;
globalThis.hostGetTradesChartsDto = hostGetTradesChartsDto;

function hostRunTradingSystem() {
  try {
    // Delegate to the canonical host entrypoint so the same inject/persist logic runs
    if (typeof RunTradingSystem !== 'function') {
      throw new Error('Host RunTradingSystem entrypoint not available');
    }
    var res = RunTradingSystem();
    return (res && typeof res === 'object') ? res : { ok: true, message: 'Run completed' };
  } catch (e) {
    try { Logger.log('[Host#hostRunTradingSystem] ' + (e && e.message ? e.message : e)); } catch(_){}
    return { ok: false, message: (e && e.message ? e.message : e) };
  }
}

function getMonitorBridge_() {
  var store = globalThis.RisontisHostPersistence && globalThis.RisontisHostPersistence.Monitor;
  if (!store || typeof store.readAll !== 'function') {
    throw new Error('[HostSidebar] Monitor persistence bridge missing (RisontisHostPersistence.Monitor.readAll).');
  }
  return store;
}

function hostGetMonitorSnapshot() {
  try {
    const props = getMonitorBridge_().readAll();
    const snap = __dpReadFullChunked__(props, 'MONITOR_SNAPSHOT_FULL') || {};
    if (
      globalThis.RisontisCoreLibrary &&
      RisontisCoreLibrary.ControlCenterServices &&
      typeof RisontisCoreLibrary.ControlCenterServices.normalizeCcuiSnapshot === 'function'
    ) {
      return RisontisCoreLibrary.ControlCenterServices.normalizeCcuiSnapshot(snap);
    }
    return snap;

  } catch (e) {
    try {
      Logger.log('[Host#hostGetMonitorSnapshot/ERR] ' + (e && e.message ? e.message : e));
    } catch (_) {}
    return { engine: {}, assets: [], trend4h: { ema: {}, adx: {} }, totals: {}, positions: [] };
  }
}

function hostGetMiniMonitorDto() {
  try {
    var monitorBridge = getMonitorBridge_();
    return monitorBridge.buildDto();
  } catch (e) {
    try { Logger.log('[HostSidebar#hostGetMiniMonitorDto] ' + (e && e.message ? e.message : e)); } catch(_){}
    return { assets: [], totals: {}, positions: [], emaDirMap: {}, adxDirMap: {}, trendMap: {} };
  }
}

// ================= Delegate ops (library calls) =================

function hostDebugArchive() {
  try {
    const dto = getHostTradesArchiveDto_();
    if (globalThis.RisontisCoreLibrary &&
        RisontisCoreLibrary.TradesService &&
        typeof RisontisCoreLibrary.TradesService.debugReadArchive_ === 'function') {
      return RisontisCoreLibrary.TradesService.debugReadArchive_(dto);
    }
    throw new Error('TradesService.debugReadArchive_ not available');
  } catch (e) {
    try { Logger.log('[Host#hostDebugArchive ERR] ' + (e && e.message ? e.message : e)); } catch(_){}
    return { ok:false, error:(e && e.message ? e.message : String(e||'unknown')) };
  }
}

function hostMapArchiveRow(raw) {
  try {
    if (globalThis.RisontisCoreLibrary &&
        RisontisCoreLibrary.TradesService &&
        typeof RisontisCoreLibrary.TradesService.mapArchiveChunkRow_ === 'function') {
      return RisontisCoreLibrary.TradesService.mapArchiveChunkRow_(raw);
    }
    throw new Error('TradesService.mapArchiveChunkRow_ not available');
  } catch (e) {
    try { Logger.log('[Host#hostMapArchiveRow ERR] ' + (e && e.message ? e.message : e)); } catch(_){}
    return { ok:false, error:(e && e.message ? e.message : String(e||'unknown')) };
  }
}

function hostDebugMerge() {
  try {
    const dto = getHostTradesArchiveDto_();
    if (globalThis.RisontisCoreLibrary &&
        RisontisCoreLibrary.TradesService &&
        typeof RisontisCoreLibrary.TradesService.debugMergeSheetAndArchive_ === 'function') {
      return RisontisCoreLibrary.TradesService.debugMergeSheetAndArchive_(dto);
    }
    throw new Error('TradesService.debugMergeSheetAndArchive_ not available');
  } catch (e) {
    try { Logger.log('[Host#hostDebugMerge ERR] ' + (e && e.message ? e.message : e)); } catch(_) {}
    return { ok:false, error:(e && e.message ? e.message : String(e||'unknown')) };
  }
}

function hostGetUnifiedTradesData(query, filters) {
  if (typeof query === 'number') {
    query = { days: query };
  }
  try {
    const dto = getHostTradesArchiveDto_();

    // Prefer new props-aware entrypoint if available
    if (globalThis.RisontisCoreLibrary &&
        RisontisCoreLibrary.TradesService &&
        typeof RisontisCoreLibrary.TradesService.getTradesDataWithProps === 'function') {
      return RisontisCoreLibrary.TradesService.getTradesDataWithProps(dto, query, filters || null);
    }

    throw new Error('TradesService.getTradesData not available');
  } catch (e) {
    try { Logger.log('[Host#hostGetUnifiedTradesData ERR] ' + (e && e.message ? e.message : e)); } catch(_){}
    return { ok: false, error: (e && e.message ? e.message : String(e || 'unknown')) };
  }
}

function hostGetTradesData(query, filters) {
  if (typeof query === 'number') { query = { days: query }; }
  return hostGetUnifiedTradesData(query, filters);
}

function hostBuildTradesDto(params) {
  try {
    var dto = getHostTradesArchiveDto_();
    params = params || {};
    params.tradesArchiveDto = dto;
    var svc = (globalThis.RisontisCoreLibrary && RisontisCoreLibrary.ControlCenterServices)
      || (typeof ControlCenterServices !== 'undefined' ? ControlCenterServices : null);
    if (!svc || typeof svc.buildTradesDto !== 'function') {
      throw new Error('ControlCenterServices.buildTradesDto not available');
    }
    return svc.buildTradesDto(params);
  } catch (e) {
    try { Logger.log('[Host#hostBuildTradesDto ERR] ' + (e && e.message ? e.message : e)); } catch(_){}
    return { ok:false, reason:(e && e.message) ? e.message : String(e||'unknown') };
  }
}

function hostGetTradesChartsDto(params) {
  try {
    var dto = getHostTradesArchiveDto_();
    params = params || {};
    params.tradesArchiveDto = dto;
    var ui = (globalThis.RisontisCoreLibrary && RisontisCoreLibrary.TradesUi)
      || (typeof TradesUi !== 'undefined' ? TradesUi : null);
    if (!ui || typeof ui.getTradesChartsDto_v1 !== 'function') {
      throw new Error('TradesUi.getTradesChartsDto_v1 not available');
    }
    return ui.getTradesChartsDto_v1(params);
  } catch (e) {
    try { Logger.log('[Host#hostGetTradesChartsDto ERR] ' + (e && e.message ? e.message : e)); } catch(_){}
    return { equity: [], dailyPnl: [], error: (e && e.message) ? e.message : String(e||'unknown') };
  }
}

function hostGetPerformanceKpis(query, filters) {
  if (typeof query === 'number') {
    query = { days: query };
  }
  try {
    const dto = getHostTradesArchiveDto_();

    // Unified dataset key (full merged dataset)
    const fullKey = 'MERGED_DATASET_FULL_9999_' + (dto.years || []).map(function(y){ return y.year + '_' + y.partKeys.length; }).join('__');
    const cache = CacheService.getScriptCache();
    const cachedJson = cache.get(fullKey);

    var merged;
    if (cachedJson) {
      try {
        merged = JSON.parse(cachedJson);
      } catch (_) {
        merged = null;
      }
    }

    if (!merged) {
      if (!(globalThis.RisontisCoreLibrary &&
            RisontisCoreLibrary.TradesService &&
            typeof RisontisCoreLibrary.TradesService.getTradesDataWithProps === 'function')) {
        throw new Error('TradesService.getTradesDataWithProps not available');
      }
      merged = RisontisCoreLibrary.TradesService.getTradesDataWithProps(dto, { days: 9999 }, null);

      try {
        cache.put(fullKey, JSON.stringify(merged), 300);
      } catch (_) { /* no-op */ }
    }

    // If query.mode === 'range', bypass days filtering and pass merged directly
    if (query && query.mode === 'range') {
      // Bypass days filtering; pass merged directly to PerformanceService
      if (!(globalThis.RisontisCoreLibrary &&
            RisontisCoreLibrary.PerformanceService &&
            typeof RisontisCoreLibrary.PerformanceService.getPerformanceKpisWithProps === 'function')) {
        throw new Error('PerformanceService.getPerformanceKpisWithProps not available');
      }
      return RisontisCoreLibrary.PerformanceService.getPerformanceKpisWithProps(dto, query, merged);
    }

    // Filter merged dataset for the requested range (only if not 'range' mode)
    var rangeDays = Number(query.days) || 30;
    var cutoff = Date.now() - (rangeDays * 24 * 60 * 60 * 1000);
    var filtered = Object.assign({}, merged);

    if (Array.isArray(merged.trades)) {
      filtered.trades = merged.trades.filter(function(t) {
        return t.sellTs && new Date(t.sellTs).getTime() >= cutoff;
      });
    }

    if (Array.isArray(merged.dailyPnl)) {
      filtered.dailyPnl = merged.dailyPnl.filter(function(p) {
        return new Date(p.date).getTime() >= cutoff;
      });
    }

    if (Array.isArray(merged.equity)) {
      filtered.equity = merged.equity.filter(function(e) {
        return new Date(e.date).getTime() >= cutoff;
      });
    }

    if (!(globalThis.RisontisCoreLibrary &&
          RisontisCoreLibrary.PerformanceService &&
          typeof RisontisCoreLibrary.PerformanceService.getPerformanceKpisWithProps === 'function')) {
      throw new Error('PerformanceService.getPerformanceKpisWithProps not available');
    }

    return RisontisCoreLibrary.PerformanceService.getPerformanceKpisWithProps(dto, query, filtered);
  } catch (e) {
    try { Logger.log('[Host#hostGetPerformanceKpis ERR] ' + (e && e.message ? e.message : e)); } catch(_){}
    return { ok:false, error:(e && e.message ? e.message : String(e||'unknown')) };
  }
}

function hostGetPerformanceKpisMulti() {
  try {
    const dto = getHostTradesArchiveDto_();
    var tz = resolveSidebarTimezone_() || (dto && dto.timezone) || 'Etc/UTC';
    const cache = CacheService.getScriptCache();
    const fullKey = 'MERGED_DATASET_FULL_9999_' + (dto.years || []).map(function(y){ return y.year + '_' + y.partKeys.length; }).join('__');

    // Load or build merged dataset once (<= 9999d)
    var merged = null;
    var cachedJson = cache.get(fullKey);
    if (cachedJson) {
      try { merged = JSON.parse(cachedJson); } catch (_) { merged = null; }
    }
    if (!merged) {
      if (!(globalThis.RisontisCoreLibrary &&
            RisontisCoreLibrary.TradesService &&
            typeof RisontisCoreLibrary.TradesService.getTradesDataWithProps === 'function')) {
        throw new Error('TradesService.getTradesDataWithProps not available');
      }
      merged = RisontisCoreLibrary.TradesService.getTradesDataWithProps(dto, { days: 9999 }, null);
      try { cache.put(fullKey, JSON.stringify(merged), 300); } catch (_) { /* ignore cache failures */ }
    }

    if (!(globalThis.RisontisCoreLibrary &&
          RisontisCoreLibrary.PerformanceService &&
          typeof RisontisCoreLibrary.PerformanceService.getPerformanceKpisWithProps === 'function')) {
      throw new Error('PerformanceService.getPerformanceKpisWithProps not available');
    }

    function filterForDays(days) {
      var cutoff = Date.now() - (Number(days) * 24 * 60 * 60 * 1000);
      var filtered = Object.assign({}, merged);
      if (Array.isArray(merged.trades)) {
        filtered.trades = merged.trades.filter(function(t){
          return t.sellTs && new Date(t.sellTs).getTime() >= cutoff;
        });
      }
      if (Array.isArray(merged.dailyPnl)) {
        filtered.dailyPnl = merged.dailyPnl.filter(function(p){
          return new Date(p.date).getTime() >= cutoff;
        });
      }
      if (Array.isArray(merged.equity)) {
        filtered.equity = merged.equity.filter(function(e){
          return new Date(e.date).getTime() >= cutoff;
        });
      }
      return filtered;
    }

    var ranges = {};
    [7, 30, 90].forEach(function(d){
      ranges[d] = RisontisCoreLibrary.PerformanceService.getPerformanceKpisWithProps(
        dto,
        { days: d },
        filterForDays(d)
      );
    });

    var updatedStr = Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd HH:mm:ss');
    return { ok: true, updated: updatedStr, timezone: tz, ranges: ranges };
  } catch (e) {
    try { Logger.log('[Host#hostGetPerformanceKpisMulti ERR] ' + (e && e.message ? e.message : e)); } catch(_){}
    return { ok:false, error:(e && e.message ? e.message : String(e||'unknown')) };
  }
}

function hostGetEquityNavData_v1(query) {
  try {
    if (typeof globalThis.hostGetEquityNavData_v2 === 'function') {
      return globalThis.hostGetEquityNavData_v2(query || {});
    }
    throw new Error('hostGetEquityNavData_v2 not available');
  } catch (e) {
    try { Logger.log('[Host#hostGetEquityNavData_v1 ERR] ' + (e && e.message ? e.message : e)); } catch(_) {}
    return { ok:false, nav:[], error:(e && e.message ? e.message : String(e||'unknown')) };
  }
}

function openConfigSheet() {
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    if (!ss) throw new Error('No active spreadsheet');
    var names = ['Settings','Config'];
    for (var i = 0; i < names.length; i++) {
      var sh = ss.getSheetByName(names[i]);
      if (sh) { ss.setActiveSheet(sh); return { ok:true, sheet: names[i] }; }
    }
    throw new Error('Settings/Config sheet not found');
  } catch (e) {
    try { Logger.log('[Host#openConfigSheet] ' + (e && e.message ? e.message : e)); } catch(_) {}
    return { ok:false, error: (e && e.message ? e.message : String(e||'unknown')) };
  }
}

function hostGetMonitorSnapshotRaw() {
  try {
    var props = getMonitorBridge_().readAll();
    var snap = __dpReadFullChunked__(props, 'MONITOR_SNAPSHOT_FULL');
    if (!snap) return '';
    try {
      var mo = props['MONITOR_OVERVIEW_V1'] ? JSON.parse(props['MONITOR_OVERVIEW_V1']) : {};
      if (mo && typeof mo === 'object') {
        snap.totals = Object.assign(snap.totals || {}, mo);
      }
    } catch (_) {}
    var payload = JSON.stringify(snap);
    try { Logger.log('[Host#hostGetMonitorSnapshotRaw] DP FULL len=' + payload.length + ' first200=' + payload.substring(0,200)); } catch(_) {}
    return payload;
  } catch (e) {
    try { Logger.log('[Host#hostGetMonitorSnapshotRaw/ERR] ' + (e && e.message ? e.message : e)); } catch(_) {}
    return '';
  }
}

function __dpReadFullChunked__(props, keyBase) {
  try {
    var m = props[keyBase + '_MANIFEST_V1'];
    if (!m) return null;
    var man = JSON.parse(m);
    var n = Number(man.parts || 0);
    if (!n) return null;
    var buf = '';
    for (var i = 1; i <= n; i++) {
      var part = props[keyBase + '_P' + i + '_V1'];
      if (!part) return null; // inconsistent write; caller may fallback
      buf += part;
    }
    return JSON.parse(buf);
  } catch (_) {
    return null;
  }
}

function ensureInstall() {
  return sidebarHandleRunSetup();
}

function repairTriggers() {
  return sidebarHandleRepairTriggers();
}

function RunStylingPass() {
  try {
    if (globalThis.RisontisCoreLibrary && typeof RisontisCoreLibrary.RunStylingPass === 'function') {
      return RisontisCoreLibrary.RunStylingPass();
    }
    if (typeof globalThis.RunStylingPass === 'function') {
      return globalThis.RunStylingPass();
    }
    return { ok: false, message: 'RunStylingPass not available' };
  } catch (e) {
    return { ok: false, message: (e && e.message ? e.message : e) };
  }
}

function getRuntimeStatus_() {
  try {
    __ccLog_('status','invoked');
    var up = PropertiesService.getUserProperties();
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var boundId = (up.getProperty(_UP_KEYS.ACTIVE_SPREADSHEET_ID) || '').trim();
    var sheetName = ss ? ss.getName() : null;
    var timeZone = ss ? ss.getSpreadsheetTimeZone() : (Session.getScriptTimeZone() || 'Etc/UTC');

    var hasKey  = !!(up.getProperty(_UP_KEYS.BITVAVO_API_KEY)   && up.getProperty(_UP_KEYS.BITVAVO_API_KEY).trim());
    var hasSec  = !!(up.getProperty(_UP_KEYS.BITVAVO_API_SECRET) && up.getProperty(_UP_KEYS.BITVAVO_API_SECRET).trim());
    var hasMail = !!(up.getProperty(_UP_KEYS.ALERT_EMAIL)        && up.getProperty(_UP_KEYS.ALERT_EMAIL).trim());
    var allSec  = hasKey && hasSec && hasMail;

    var all = ScriptApp.getProjectTriggers() || [];
    var present = new Set(all.map(function(t){ return t.getHandlerFunction && t.getHandlerFunction(); }));
    var missing = _HANDLERS.filter(function(h){ return !present.has(h); });
    var trigSummary = (missing.length === 0)
      ? 'All present (' + _HANDLERS.length + ')'
      : 'Missing: ' + missing.join(', ');

    __ccLog_('status','ok; triggers=' + (all ? all.length : 0));
    return {
      libVersion: __readLibraryVersion_(),
      ok: true,
      projectId: (ScriptApp.getScriptId && ScriptApp.getScriptId()) || null,
      sheetName: sheetName,
      timeZone: timeZone,
      boundId: boundId || null,
      secrets: { BITVAVO_API_KEY: hasKey, BITVAVO_API_SECRET: hasSec, ALERT_EMAIL: hasMail, all: allSec },
      enableAllowed: !!allSec,
      triggers: { summary: trigSummary, total: _HANDLERS.length, missing: missing },
      license: hostGetLicenseStatus()
    };
  } catch (e) {
    __ccLog_('status/ERR', (e && e.message ? e.message : e));
    return { ok: false, error: (e && e.message ? e.message : String(e||'unknown')) };
  }
}

function getUserProps_() {
  var up = PropertiesService.getUserProperties();
  return {
    BITVAVO_API_KEY: !!(up.getProperty(_UP_KEYS.BITVAVO_API_KEY) && up.getProperty(_UP_KEYS.BITVAVO_API_KEY).trim()),
    BITVAVO_API_SECRET: !!(up.getProperty(_UP_KEYS.BITVAVO_API_SECRET) && up.getProperty(_UP_KEYS.BITVAVO_API_SECRET).trim()),
    ALERT_EMAIL: !!(up.getProperty(_UP_KEYS.ALERT_EMAIL) && up.getProperty(_UP_KEYS.ALERT_EMAIL).trim()),
    ACTIVE_SPREADSHEET_ID: up.getProperty(_UP_KEYS.ACTIVE_SPREADSHEET_ID) || null
  };
}

function getEffectiveUserEmail_() {
  try {
    var user = Session.getEffectiveUser && Session.getEffectiveUser();
    var email = user && typeof user.getEmail === 'function' ? user.getEmail() : '';
    return (email && email.indexOf('@') > 0) ? email : '';
  } catch (_e) {
    return '';
  }
}

function setUserProps_(payload) {
  if (!payload || typeof payload !== 'object') {
    throw new Error('setUserProps_: invalid payload');
  }
  var up = PropertiesService.getUserProperties();
  ['BITVAVO_API_KEY','BITVAVO_API_SECRET','ALERT_EMAIL'].forEach(function(k){
    if (Object.prototype.hasOwnProperty.call(payload, k)) {
      var v = (payload[k] == null ? '' : String(payload[k])).trim();
      if (k === 'ALERT_EMAIL') {
        try { v = getEffectiveUserEmail_() || v; } catch (_) {}
      }
      // Do not log values; only presence flags are exposed elsewhere
      if (v.length > 0) up.setProperty(_UP_KEYS[k], v); else up.deleteProperty(_UP_KEYS[k]);
    }
  });
  return { ok: true };
}

function sidebarSaveSecrets(payload) {
  try {
    const up = PropertiesService.getUserProperties();
    const saved = [];
    if (payload && typeof payload === 'object') {
      if (payload.apiKey && typeof payload.apiKey === 'string' && payload.apiKey.trim() !== '') {
        up.setProperty(_UP_KEYS.BITVAVO_API_KEY, payload.apiKey.trim());
        saved.push('apiKey');
      }
      if (payload.apiSecret && typeof payload.apiSecret === 'string' && payload.apiSecret.trim() !== '') {
        up.setProperty(_UP_KEYS.BITVAVO_API_SECRET, payload.apiSecret.trim());
        saved.push('apiSecret');
      }
    }
    if (saved.length === 0) return { ok: false, reason: 'Nothing to save' };
    return { ok: true, saved };
  } catch (e) {
    return { ok: false, reason: e && e.message ? e.message : String(e || 'Unknown') };
  }
}
globalThis.sidebarSaveSecrets = sidebarSaveSecrets;

function validateSecrets(payload) {
  if (globalThis.RisontisCoreLibrary &&
      typeof RisontisCoreLibrary.validateSecrets === 'function') {
    return RisontisCoreLibrary.validateSecrets(payload);
  }

  return { ok: false, reason: 'validateSecrets not available in host context' };
}
globalThis.validateSecrets = validateSecrets;
globalThis.hostGetEquityNavData_v1 = hostGetEquityNavData_v1;

function hostGetAvailableRange() {
  try {
    const navDto = (typeof hostGetEquityNavDto_ === 'function') ? hostGetEquityNavDto_() : null;
    const tradesDto = getHostTradesArchiveDto_();
    var earliest = null;
    var latest = null;

    // --- NAV (DP) ---
    try {
      var rows = (navDto && Array.isArray(navDto.rows)) ? navDto.rows : [];
      if (!rows.length && navDto && navDto.summary && navDto.summary.first && navDto.summary.last) {
        earliest = navDto.summary.first;
        latest = navDto.summary.last;
      } else if (rows.length) {
        earliest = rows[0].dateISO || rows[0].date || null;
        latest = rows[rows.length - 1].dateISO || rows[rows.length - 1].date || null;
      }
    } catch (_) {
      earliest = earliest || null;
      latest = latest || null;
    }

    // --- Trades (merged) ---
    try {
      if (globalThis.RisontisCoreLibrary &&
          RisontisCoreLibrary.TradesService &&
          typeof RisontisCoreLibrary.TradesService.getTradesDataWithProps === 'function') {

        var merged = RisontisCoreLibrary.TradesService.getTradesDataWithProps(tradesDto, { days: 9999 }, null);
        if (merged && Array.isArray(merged.trades)) {
          merged.trades.forEach(function(t){
            var d = t.sellTs || t.buyTs;
            if (!d) return;
            var iso = String(d).slice(0,10);
            if (iso && iso.length === 10) {
              if (!earliest || iso < earliest) earliest = iso;
              if (!latest || iso > latest) latest = iso;
            }
          });
        }
      }
    } catch (_) {}

    var hasArchive = !!earliest;

    return {
      ok: true,
      earliest: earliest,
      latest: latest,
      hasArchive: hasArchive
    };

  } catch (e) {
    return { ok:false, earliest:null, latest:null, hasArchive:false };
  }
}
globalThis.hostGetAvailableRange = hostGetAvailableRange;

// ================= Delegate ops (library calls) =================

function sidebarHandleRunSetup() {
  ensureActiveSpreadsheetBinding_();
  ensureActiveSheetMatchesTarget_();

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  if (ss) {
    var desiredName = 'Risontis';
    var currentName = String(ss.getName() || '').trim();
    if (currentName !== desiredName) {
      ss.rename(desiredName);
    }
  }

  // Keep folder structure creation; avoid Drive API moves on the active sheet.
  ensureAppFolderBinding_();
  materializeSetupFolders_((typeof GLOBAL_CONFIG !== 'undefined' ? GLOBAL_CONFIG : null));
  __ccLog_('setup','invoked');
  var res = null;
  // 1) Library alias (primary). Do not swallow exceptions; let UI failure handler show the error.
  if (globalThis.RisontisCoreLibrary &&
      RisontisCoreLibrary.SetupRunner &&
      typeof RisontisCoreLibrary.SetupRunner.ensureInstall === 'function') {
    res = RisontisCoreLibrary.SetupRunner.ensureInstall((typeof GLOBAL_CONFIG !== 'undefined' ? GLOBAL_CONFIG : null));
  }
  else if (typeof globalThis.SetupRunner !== 'undefined' &&
      typeof globalThis.SetupRunner.ensureInstall === 'function') {
    res = globalThis.SetupRunner.ensureInstall((typeof GLOBAL_CONFIG !== 'undefined' ? GLOBAL_CONFIG : null));
  } else if (typeof hostRunSetup === 'function') {
    res = hostRunSetup();
  } else {
    throw new Error('Run Setup failed: SetupRunner.ensureInstall not available and hostRunSetup() missing.');
  }

  // After successful provisioning, ensure triggers are installed/repaired (same authorized context)
  var triggersOk = false;
  if (typeof setupAllRequiredTriggers === 'function') {
    setupAllRequiredTriggers();
    triggersOk = true;
  } else if (
    globalThis.RisontisCoreLibrary &&
    RisontisCoreLibrary.SetupRunner &&
    typeof RisontisCoreLibrary.SetupRunner.repairTriggers === 'function'
  ) {
    RisontisCoreLibrary.SetupRunner.repairTriggers((typeof GLOBAL_CONFIG !== 'undefined' ? GLOBAL_CONFIG : null));
    triggersOk = true;
  } else if (typeof SetupRunner !== 'undefined' && typeof SetupRunner.repairTriggers === 'function') {
    SetupRunner.repairTriggers((typeof GLOBAL_CONFIG !== 'undefined' ? GLOBAL_CONFIG : null));
    triggersOk = true;
  }
  if (!triggersOk) {
    throw new Error('Setup completed but no trigger installer was available.');
  }

  return res;
}

// Host menu entrypoint: thin wrapper to canonical setup path with basic feedback.
function hostRunSetupFromMenu() {
  try {
    // Pre-install toast
    try {
      SpreadsheetApp.getActive().toast('This may take a moment', 'Installing Risontis…', -1);
    } catch (_) {}
    var res = sidebarHandleRunSetup();
    // Post-install toast (no reload/refresh instructions)
    try { SpreadsheetApp.getActive().toast('Setup completed. Final initialisation is now in progress.'); } catch (_) {}
    // Open Library Control Center after install
    openLibraryControlCenter();
    return res;
  } catch (e) {
    try {
      var ui = SpreadsheetApp.getUi();
      ui.alert('Setup failed', (e && e.message) ? e.message : String(e || 'unknown'), ui.ButtonSet.OK);
    } catch (_) {}
    throw e;
  }
}
globalThis.hostRunSetupFromMenu = hostRunSetupFromMenu;

function sidebarHandleRepairTriggers() {
  ensureActiveSpreadsheetBinding_();
  __ccLog_('repair','invoked');
  try {
    if (
      globalThis.RisontisCoreLibrary &&
      RisontisCoreLibrary.SetupRunner &&
      typeof RisontisCoreLibrary.SetupRunner.repairTriggers === 'function'
    ) {
      const res = RisontisCoreLibrary.SetupRunner.repairTriggers((typeof GLOBAL_CONFIG !== 'undefined' ? GLOBAL_CONFIG : null));
      return (res && typeof res === 'object') ? res : { ok: true };
    }
    if (typeof SetupRunner !== 'undefined' && typeof SetupRunner.repairTriggers === 'function') {
      return SetupRunner.repairTriggers((typeof GLOBAL_CONFIG !== 'undefined' ? GLOBAL_CONFIG : null));
    }
    return { ok:false, error:'SetupRunner.repairTriggers not available via library.' };
  } catch (e) {
    return { ok:false, error:'Repair Triggers failed: ' + (e && e.message ? e.message : e) };
  }
}

function getTradingEnabledFlag() {
  try {
    const dp = PropertiesService.getDocumentProperties();
    var v = dp.getProperty('TRADING_ENABLED');
    if (v == null || v === '') return true; // default to enabled
    v = String(v).toLowerCase();
    return (v === 'true' || v === '1');
  } catch (e) {
    try { Logger.log('[HostSidebar#getTradingEnabledFlag/ERR] ' + (e && e.message ? e.message : e)); } catch(_) {}
    return true; // safe default
  }
}

function setTradingEnabledFlag(enabled) {
  try {
    const dp = PropertiesService.getDocumentProperties();
    var flag = !!enabled;
    dp.setProperty('TRADING_ENABLED', String(flag));
    return { ok: true, tradingEnabled: flag };
  } catch (e) {
    try { Logger.log('[HostSidebar#setTradingEnabledFlag/ERR] ' + (e && e.message ? e.message : e)); } catch(_) {}
    return { ok: false, error: (e && e.message ? e.message : String(e||'unknown')) };
  }
}

// Open Monitor dialog: build DTO from HOST DP and render via library (FULL snapshot only)
function openMonitorDialog() {
  try {
    var dto = getMonitorBridge_().buildDto();

    // Validate library renderer
    if (!(globalThis.RisontisCoreLibrary &&
          RisontisCoreLibrary.MonitorUi &&
          typeof RisontisCoreLibrary.MonitorUi.buildMonitorDashboardHtmlFromDto_v1 === 'function')) {
      throw new Error('MonitorUi renderer not available on library alias');
    }

    var htmlString = String(RisontisCoreLibrary.MonitorUi.buildMonitorDashboardHtmlFromDto_v1(dto));
    var html = HtmlService.createHtmlOutput(htmlString)
      .setTitle('\u200B')
      .setWidth(1600)
      .setHeight(1350);
    SpreadsheetApp.getUi().showModalDialog(html, "\u200B");
  } catch (e) {
    try { Logger.log('[HostSidebar#openMonitorDialog] ' + (e && e.message ? e.message : e)); } catch(_) {}
    throw e;
  }
}

// Host RPC: open License Manager dialog
function openLicenseManager() {
  try {
    if (
      !globalThis.RisontisCoreLibrary ||
      typeof RisontisCoreLibrary.buildLicenseManagerHtmlString !== 'function'
    ) {
      throw new Error('buildLicenseManagerHtmlString not available');
    }

    var htmlString = String(
      RisontisCoreLibrary.buildLicenseManagerHtmlString()
    );

    var html = HtmlService
      .createHtmlOutput(htmlString)
      .setTitle('\u200B')
      .setWidth(520)
      .setHeight(620);

    SpreadsheetApp.getUi().showModalDialog(html, "\u200B");
    return { ok: true };
  } catch (e) {
    try {
      Logger.log(
        '[HostSidebar#openLicenseManager ERR] ' +
        (e && e.message ? e.message : e)
      );
    } catch (_){}
    return {
      ok: false,
      error: (e && e.message ? e.message : String(e || 'unknown'))
    };
  }
}

function hostStartTrialStripe() {
  try {
    // Determine email
    var email = '';
    try { email = Session.getActiveUser().getEmail() || ''; } catch (_) {}
    if (!email) throw new Error('Unable to determine user email');

    // Determine sheetId
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    if (!ss) throw new Error('No active spreadsheet');
    var sheetId = ss.getId();

    // Backend endpoint
    var url = LICENSE_BACKEND_BASE + '/startTrialStripe'
      + '?email=' + encodeURIComponent(email)
      + '&sheetId=' + encodeURIComponent(sheetId);

    // Invoke backend to start trial + create onboarding session
    var resp = UrlFetchApp.fetch(url, { method:'post', muteHttpExceptions:true });
    var code = resp.getResponseCode();
    var body = resp.getContentText();

    if (code < 200 || code >= 300) {
      return { ok:false, error:'Backend error (' + code + '): ' + body };
    }

    // Parse backend response
    var out = null;
    try { out = JSON.parse(body); } catch (_) { out = null; }

    if (!out || !out.ok || !out.url) {
      return { ok:false, error:'Invalid backend response: ' + body };
    }

    // Host returns Stripe Onboarding URL to UI for redirect
    return { ok:true, url: out.url };

  } catch (e) {
    try { Logger.log('[HostSidebar#hostStartTrialStripe ERR] ' + (e && e.message ? e.message : e)); } catch(_){}
    return { ok:false, error: (e && e.message ? e.message : String(e||'unknown')) };
  }
}

function hostGetStripePrices() {
  try {
    // Determine email (no-op but can be useful for future filtering)
    var email = '';
    try { email = Session.getActiveUser().getEmail() || ''; } catch (_) {}

    var url = LICENSE_BACKEND_BASE + '/listPrices';

    var resp = UrlFetchApp.fetch(url, { method: 'get', muteHttpExceptions: true });
    var code = resp.getResponseCode();
    var body = resp.getContentText();

    if (code < 200 || code >= 300) {
      return { ok:false, error:'Backend error (' + code + '): ' + body };
    }

    var out = {};
    try { out = JSON.parse(body); } catch (_) { out = {}; }

    if (!out || !out.ok || !Array.isArray(out.tiers)) {
      return { ok:false, error:'Invalid backend response: ' + body };
    }

    return { ok:true, tiers: out.tiers };

  } catch (e) {
    try { Logger.log('[HostSidebar#hostGetStripePrices ERR] ' + (e && e.message ? e.message : e)); } catch(_){}
    return { ok:false, error:(e && e.message ? e.message : String(e||'unknown')) };
  }
}
globalThis.hostGetStripePrices = hostGetStripePrices;

function hostStartCheckoutSession(priceIdOrTier) {
  try {
    // Determine email
    var email = '';
    try { email = Session.getActiveUser().getEmail() || ''; } catch (_) {}
    if (!email) throw new Error('Unable to determine user email');

    // Determine sheetId
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    if (!ss) throw new Error('No active spreadsheet');
    var sheetId = ss.getId();

    // Load existing license to forward Stripe customerId
    var dp = PropertiesService.getDocumentProperties();
    var licRaw = dp.getProperty('LICENSE_JSON');
    var existing = null;
    try { existing = licRaw ? JSON.parse(licRaw) : null; } catch (_) { existing = null; }
    var customerId = existing && existing.stripeCustomerId ? existing.stripeCustomerId : '';

    // Backend endpoint
    var url =
      LICENSE_BACKEND_BASE + '/createCheckoutSession'
      + '?email=' + encodeURIComponent(email)
      + '&sheetId=' + encodeURIComponent(sheetId)
      + '&priceId=' + encodeURIComponent(priceIdOrTier)
      + (customerId ? ('&customerId=' + encodeURIComponent(customerId)) : '');

    var resp = UrlFetchApp.fetch(url, { method:'post', muteHttpExceptions:true });
    var code = resp.getResponseCode();
    var body = resp.getContentText();

    if (code < 200 || code >= 300) {
      return { ok:false, error:'Backend error (' + code + '): ' + body };
    }

    var out = {};
    try { out = JSON.parse(body); } catch (_) { out = {}; }
    if (!out || !out.url) {
      return { ok:false, error:'Invalid backend response: ' + body };
    }

    return { ok:true, url: out.url };

  } catch (e) {
    return { ok:false, error:(e && e.message ? e.message : String(e||'unknown')) };
  }
}

globalThis.hostStartCheckoutSession = hostStartCheckoutSession;

function hostUpdateLicense(priceId) {
  try {
    // Determine email
    var email = '';
    try { email = Session.getActiveUser().getEmail() || ''; } catch (_) {}
    if (!email) throw new Error('Unable to determine user email');

    // Determine sheetId
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    if (!ss) throw new Error('No active spreadsheet');
    var sheetId = ss.getId();

    if (!priceId) throw new Error('Missing priceId');

    var url =
      LICENSE_BACKEND_BASE + '/updateSubscription'
      + '?email=' + encodeURIComponent(email)
      + '&sheetId=' + encodeURIComponent(sheetId)
      + '&priceId=' + encodeURIComponent(priceId);

    var resp = UrlFetchApp.fetch(url, { method: 'post', muteHttpExceptions: true });
    var code = resp.getResponseCode();
    var body = resp.getContentText();

    if (code < 200 || code >= 300) {
      return { ok:false, error:'Backend error (' + code + '): ' + body };
    }

    var out = {};
    try { out = JSON.parse(body); } catch (_) { out = {}; }

    return out;

  } catch (e) {
    return { ok:false, error:(e && e.message ? e.message : String(e||'unknown')) };
  }
}

function hostOpenCustomerPortal() {
  try {
    // Determine email
    var email = '';
    try { email = Session.getActiveUser().getEmail() || ''; } catch (_) {}
    if (!email) throw new Error('Unable to determine user email');

    // Determine sheetId
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    if (!ss) throw new Error('No active spreadsheet');
    var sheetId = ss.getId();

    var url =
      LICENSE_BACKEND_BASE + '/createPortalSession'
      + '?email=' + encodeURIComponent(email)
      + '&sheetId=' + encodeURIComponent(sheetId);

    var resp = UrlFetchApp.fetch(url, { method:'post', muteHttpExceptions:true });
    var code = resp.getResponseCode();
    var body = resp.getContentText();

    if (code < 200 || code >= 300) {
      return { ok:false, error:'Backend error (' + code + '): ' + body };
    }

    var out = {};
    try { out = JSON.parse(body); } catch (_) { out = {}; }
    return out;

  } catch (e) {
    return { ok:false, error:(e && e.message ? e.message : String(e||'unknown')) };
  }
}
globalThis.hostOpenCustomerPortal = hostOpenCustomerPortal;

function hostRedirectToStripe(url) {
  try {
    // UI-side redirect only (HtmlService modal sandbox workaround).
    // Host does NOT open any modal or window. All redirect logic is handled in the LicenseManager UI.
    if (!url || typeof url !== 'string') {
      return { ok:false, error:'Invalid URL for redirect' };
    }
    return { ok:true, url:url };
  } catch (e) {
    try { Logger.log('[HostSidebar#hostRedirectToStripe ERR] ' + (e && e.message ? e.message : e)); } catch(_){}
    return { ok:false, error:(e && e.message ? e.message : String(e||"unknown")) };
  }
}
globalThis.hostRedirectToStripe = hostRedirectToStripe;

function hostBuildFullChartsHtmlFromRange(rangeDays) {
  var days = Number(rangeDays) || 7;
  var dto = {
    rangeDays: days,
    tz: 'Etc/UTC',
    updated: new Date().toISOString()
  };
  if (!(globalThis.RisontisCoreLibrary &&
        typeof RisontisCoreLibrary.buildFullChartsHtmlFromDto_v1 === 'function')) {
    throw new Error('Library provider missing: buildFullChartsHtmlFromDto_v1');
  }
  return String(RisontisCoreLibrary.buildFullChartsHtmlFromDto_v1(dto));
}
globalThis.hostBuildFullChartsHtmlFromRange = hostBuildFullChartsHtmlFromRange;

// Open Trades/Charts dialog quickly, then async-fetch full HTML via google.script.run
function openTradesDialogWithRange(rangeDays) {
  var days = Number(rangeDays) || 7;
  try {
    var htmlString = hostBuildFullChartsHtmlFromRange(days);
    var out = HtmlService.createHtmlOutput(htmlString)
      .setTitle('\u200B')
      .setWidth(1600)
      .setHeight(1350);
    SpreadsheetApp.getUi().showModalDialog(out, '\u200B');
  } catch (e) {
    try { Logger.log('[Host#openTradesDialogWithRange] ' + (e && e.message ? e.message : e)); } catch(_) {}
    throw e;
  }
}

function openChartsDashboardWithRange(days) {
  var d = Number(days);
  if (!isFinite(d) || d <= 0) d = 30;
  return openTradesDialogWithRange(d);
}

function onEdit(e){
  try{
    var r = e && e.range; if (!r) return;
    var sh = r.getSheet(); if (!sh || sh.getName() !== 'Settings') return;
    if (globalThis.RisontisCoreLibrary &&
        RisontisCoreLibrary.SettingsResolver &&
        typeof RisontisCoreLibrary.SettingsResolver.onActiveProfileEdit === 'function') {
      RisontisCoreLibrary.SettingsResolver.onActiveProfileEdit();
    }
  }catch(_){ /* no-op */ }
}
// Expose manual invalidator for archive DTO cache (optional hook for refresh flows)
function invalidateHostTradesArchiveDtoCache() {
  try { invalidateHostTradesArchiveDtoCache_(); return { ok:true }; }
  catch(e){ return { ok:false, error:(e && e.message) ? e.message : String(e||'unknown') }; }
}
globalThis.invalidateHostTradesArchiveDtoCache = invalidateHostTradesArchiveDtoCache;
