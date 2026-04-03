type LogLevel = "debug" | "info" | "warn" | "error";

const LEVEL_ORDER: Record<LogLevel, number> = {
  debug: 10, info: 20, warn: 30, error: 40,
};

const CURRENT_LEVEL: LogLevel = (process.env.LOG_LEVEL as LogLevel) || "info";

function shouldLog(level: LogLevel) {
  return LEVEL_ORDER[level] >= LEVEL_ORDER[CURRENT_LEVEL];
}

function ts() {
  return new Date().toISOString().slice(0, 19);
}

export const log = {
  debug(code: string, msg: string, meta?: Record<string, unknown>) {
    if (!shouldLog("debug")) return;
    console.log(`[${ts()}] DEBUG ${code} — ${msg}`, meta ?? "");
  },
  info(code: string, msg: string, meta?: Record<string, unknown>) {
    if (!shouldLog("info")) return;
    console.log(`[${ts()}] INFO  ${code} — ${msg}`, meta ?? "");
  },
  warn(code: string, msg: string, meta?: Record<string, unknown>) {
    if (!shouldLog("warn")) return;
    console.warn(`[${ts()}] WARN  ${code} — ${msg}`, meta ?? "");
  },
  error(code: string, msg: string, meta?: Record<string, unknown>) {
    if (!shouldLog("error")) return;
    console.error(`[${ts()}] ERROR ${code} — ${msg}`, meta ?? "");
  },
};
