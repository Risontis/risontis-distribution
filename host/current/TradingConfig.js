/****
 * File: TradingConfigRisontisCore.js
 * Description: Lean host config shell; runtime merges inject defaults via SettingsResolver + library constants.
 * Version: 2.0.0
 * Date: 2025-12-10
 */

// Global configuration object (lean)
var CONFIG = {
  // Injected at runtime from Assets.js by the library. Do NOT define CoinGroups here.
  // CoinGroups: {},

  // Domains intentionally left empty to allow clean merge of Settings (tenant) and Constants (defaults/invariants).
  Indicators: {},
  Risk: {},
  SLTPControl: {},
  TradeLimits: {},
  Execution: {},
  Cooldowns: {},
  System: {},
  Logging: {},
  API: {}
};

// Backward‑compatibility alias for legacy code paths. Do not mutate elsewhere.
var GLOBAL_CONFIG = CONFIG;

// Smoke test helper
function testConfigLoad() {
  Logger.log('CONFIG domains (lean): ' + Object.keys(CONFIG).join(', '));
}
