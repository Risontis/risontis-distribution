/****
 * File: SetupTriggers.js
 * Description: Host trigger installer (idempotent time-based cadence binding).
 * Version: 2.0.0
 * Date: 2025-12-10
 */

// Centralized handler names to avoid drift
const HANDLERS = Object.freeze({
  RUN_TRADING: 'RunTradingSystem',
  MONTHLY_ARCHIVE: 'runMonthlyArchive',
  ROTATE_SYSLOG_DAILY: 'rotateSystemLogSheet',
  STYLE_SHEETS: 'runSheetStyling',
  EQUITY_ROTATOR: 'runEquityRotator',
  ARCHIVE_REFRESH: 'runArchiveCachesRefresh'
});

// Installable onOpen handler (authorized context)
const ON_OPEN_HANDLER = 'onOpenAutoOpen_';

// Idempotent install for all required time-based triggers.
function setupAllRequiredTriggers() {
  // --- Robust config (with fallbacks) ---
  if (typeof GLOBAL_CONFIG === 'undefined') {
    throw new Error('setupAllRequiredTriggers: GLOBAL_CONFIG is undefined');
  }
  const cfg = GLOBAL_CONFIG;
  var tz = (cfg && cfg.Logging && cfg.Logging.timeZone) || '';
  if (!tz) {
    try {
      tz = SpreadsheetApp.getActive().getSpreadsheetTimeZone() || Session.getScriptTimeZone() || 'Etc/UTC';
      Logger.log('[Triggers] WARN: Logging.timeZone missing; falling back to sheet/script TZ: ' + tz);
    } catch (e) {
      tz = 'Etc/UTC';
      Logger.log('[Triggers] WARN: Timezone fallback to Etc/UTC');
    }
  }

  // Cadence from library-owned defaults (no TradingConfig dependency)
  var cron = 5;
  try {
    if (typeof RisontisCoreLibrary !== 'undefined' &&
        RisontisCoreLibrary &&
        typeof RisontisCoreLibrary.getMonitorWriterCronMinutes === 'function') {
      cron = Number(RisontisCoreLibrary.getMonitorWriterCronMinutes()) || 5;
    }
  } catch (e) {
    cron = 5;
  }

  // Build the set idempotently (skip if already present)
  ensureTrigger_(HANDLERS.RUN_TRADING, () =>
    ScriptApp.newTrigger(HANDLERS.RUN_TRADING)
      .timeBased()
      .everyMinutes(5), tz);

  // First day of month, between 01:00–02:00 → schedule at 01:00
  ensureTrigger_(HANDLERS.MONTHLY_ARCHIVE, () =>
    ScriptApp.newTrigger(HANDLERS.MONTHLY_ARCHIVE)
      .timeBased()
      .onMonthDay(1)
      .atHour(1), tz);

  // Daily just after midnight (00:01) to ensure full-day logs are captured
  ensureTrigger_(HANDLERS.ROTATE_SYSLOG_DAILY, () =>
    ScriptApp.newTrigger(HANDLERS.ROTATE_SYSLOG_DAILY)
      .timeBased()
      .atHour(0)
      .nearMinute(1)
      .everyDays(1), tz);

  ensureTrigger_(HANDLERS.EQUITY_ROTATOR, () =>
    ScriptApp.newTrigger(HANDLERS.EQUITY_ROTATOR)
      .timeBased()
      .atHour(0)
      .nearMinute(11)
      .everyDays(1), tz);

  ensureTrigger_(HANDLERS.ARCHIVE_REFRESH, () =>
    ScriptApp.newTrigger(HANDLERS.ARCHIVE_REFRESH)
      .timeBased()
      .everyDays(1)
      .atHour(0), tz);

  ensureTrigger_(HANDLERS.STYLE_SHEETS, () =>
    ScriptApp.newTrigger(HANDLERS.STYLE_SHEETS)
      .timeBased()
      .everyMinutes(10), tz);

  // Ensure installable onOpen (authorized) for auto-open Control Center
  ensureInstallableOnOpenTrigger_();

  Logger.log(`[Triggers] Ensured:
  - RunTradingSystem: every 5 minutes
  - runMonthlyArchive: first day of month at 01:00
  - rotateSystemLogSheet: daily at 00:01 (${tz})
  - runEquityRotator: daily at 00:11
  - runArchiveCachesRefresh: daily at 00:00
  - runSheetStyling: every 10 minutes`);

  // Consistency check: log actual project triggers
  logCurrentTriggers_(tz);
}

// Ensures a handler has exactly one project trigger (builder bound to tz).
function ensureTrigger_(handler, builder, tz) {
  const existing = ScriptApp.getProjectTriggers()
    .some(t => t.getHandlerFunction() === handler);
  if (existing) return;

  const b = builder();
  if (!b || typeof b.inTimezone !== 'function') {
    throw new Error(`ensureTrigger_: invalid builder for handler ${handler}`);
  }
  b.inTimezone(tz).create();
}

// Ensure an installable onOpen trigger exists for onOpenAutoOpen_.
function ensureInstallableOnOpenTrigger_() {
  try {
    const ssId = SpreadsheetApp.getActive().getId();
    const exists = ScriptApp.getProjectTriggers().some(t =>
      t.getHandlerFunction && t.getHandlerFunction() === ON_OPEN_HANDLER &&
      t.getEventType && t.getEventType() === ScriptApp.EventType.ON_OPEN
    );
    if (!exists) {
      ScriptApp.newTrigger(ON_OPEN_HANDLER).forSpreadsheet(ssId).onOpen().create();
      Logger.log('[Triggers] Created installable onOpen trigger for ' + ON_OPEN_HANDLER);
    }
  } catch (e) {
    Logger.log('[Triggers] Failed to ensure installable onOpen trigger: ' + (e && e.message ? e.message : e));
  }
}

// Optional cleanup: remove duplicate triggers per handler.
function dedupeTriggersOptional() {
  const handlers = Object.values(HANDLERS);
  const legacy = [];
  const targetHandlers = handlers.concat(legacy);
  const all = ScriptApp.getProjectTriggers();
  targetHandlers.forEach(h => {
    const list = all.filter(t => t.getHandlerFunction() === h);
    if (list.length <= 1) return;
    for (let i = 1; i < list.length; i++) {
      ScriptApp.deleteTrigger(list[i]);
    }
    Logger.log(`[Triggers] Deduped extra triggers for ${h}: removed ${list.length - 1}`);
  });
}

// Logs current triggers (handler + source) for visibility.
function logCurrentTriggers_(tz) {
  try {
    const triggers = ScriptApp.getProjectTriggers();
    if (!triggers || !triggers.length) {
      Logger.log('[Triggers] No project triggers found.');
      return;
    }
    Logger.log(`[Triggers] Current project triggers (${triggers.length}) [TZ=${tz}]:`);
    triggers.forEach((t, i) => {
      const h = t.getHandlerFunction();
      const source = (typeof t.getTriggerSource === 'function') ? t.getTriggerSource() : 'n/a';
      Logger.log(`  [${i + 1}] handler="${h}" (${Object.values(HANDLERS).includes(h) ? 'known' : 'unknown'}) source=${source}`);
    });
  } catch (e) {
    Logger.log('[Triggers] Failed to enumerate triggers: ' + (e && e.message));
  }
}