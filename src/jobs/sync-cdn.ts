import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import { log } from "../logger";
import {
  R2_ACCOUNT_ID,
  R2_ACCESS_KEY_ID,
  R2_SECRET_ACCESS_KEY,
  R2_BUCKET_NAME,
} from "../config";
import { Readable } from "stream";
import { createWriteStream, createReadStream } from "fs";
import { mkdir, readdir, stat, rm, readFile } from "fs/promises";
import { join, relative, extname } from "path";
import { tmpdir } from "os";
import { pipeline } from "stream/promises";
import { createGunzip } from "zlib";
import * as tar from "tar"; // bun supports this natively via node compat

// ── S3 client for Cloudflare R2 ─────────────────────────────────
const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
});

const VERSIONS_URL = "https://ddragon.leagueoflegends.com/api/versions.json";
const DRAGONTAIL_URL = (v: string) =>
  `https://ddragon.leagueoflegends.com/cdn/dragontail-${v}.tgz`;
const MARKER_KEY = "_current_version.txt";

// ── MIME types for proper Content-Type headers ──────────────────
const MIME: Record<string, string> = {
  ".json": "application/json",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".svg": "image/svg+xml",
  ".css": "text/css",
  ".js": "application/javascript",
  ".html": "text/html",
  ".txt": "text/plain",
  ".mp3": "audio/mpeg",
  ".ogg": "audio/ogg",
};

function contentType(filePath: string): string {
  return MIME[extname(filePath).toLowerCase()] ?? "application/octet-stream";
}

// ── Helpers ─────────────────────────────────────────────────────

async function getLatestVersion(): Promise<string> {
  const res = await fetch(VERSIONS_URL, { signal: AbortSignal.timeout(10_000) });
  if (!res.ok) throw new Error(`Failed to fetch versions: ${res.status}`);
  const versions = (await res.json()) as string[];
  if (!Array.isArray(versions) || versions.length === 0)
    throw new Error("Versions array is empty");
  return versions[0];
}

async function getCurrentDeployedVersion(): Promise<string | null> {
  try {
    const res = await s3.send(
      new GetObjectCommand({ Bucket: R2_BUCKET_NAME, Key: MARKER_KEY }),
    );
    const body = await res.Body?.transformToString();
    return body?.trim() ?? null;
  } catch {
    return null; // marker doesn't exist yet
  }
}

async function downloadTgz(url: string, dest: string): Promise<void> {
  log.info("CDN_SYNC", `Downloading ${url}...`);
  const res = await fetch(url, { signal: AbortSignal.timeout(300_000) }); // 5 min timeout
  if (!res.ok) throw new Error(`Download failed: ${res.status} ${res.statusText}`);

  const fileStream = createWriteStream(dest);
  await pipeline(Readable.fromWeb(res.body as any), fileStream);

  const size = (await stat(dest)).size;
  log.info("CDN_SYNC", `Downloaded ${(size / 1024 / 1024).toFixed(1)} MB`);
}

async function extractTgz(tgzPath: string, outDir: string): Promise<void> {
  log.info("CDN_SYNC", `Extracting to ${outDir}...`);
  await mkdir(outDir, { recursive: true });
  await tar.extract({ file: tgzPath, cwd: outDir });
  log.info("CDN_SYNC", "Extraction complete");
}

async function* walkDir(dir: string): AsyncGenerator<string> {
  const entries = await readdir(dir, { withFileTypes: true });
  for (const entry of entries) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      yield* walkDir(full);
    } else {
      yield full;
    }
  }
}

async function uploadToR2(
  localDir: string,
  version: string,
): Promise<number> {
  let count = 0;
  const BATCH_SIZE = 50; // concurrent uploads
  let batch: Promise<void>[] = [];

  for await (const filePath of walkDir(localDir)) {
    const rel = relative(localDir, filePath).replace(/\\/g, "/");

    // The dragontail extracts into a folder named after the version (e.g. "16.7.1/")
    // We upload files preserving that structure so CDN paths like
    // cdn2.loldata.cc/16.7.1/img/champion/Ahri.png work correctly.
    const key = rel;

    const upload = async () => {
      const body = await readFile(filePath);
      await s3.send(
        new PutObjectCommand({
          Bucket: R2_BUCKET_NAME,
          Key: key,
          Body: body,
          ContentType: contentType(filePath),
          CacheControl: "public, max-age=31536000, immutable",
        }),
      );
    };

    batch.push(upload());
    if (batch.length >= BATCH_SIZE) {
      await Promise.all(batch);
      count += batch.length;
      if (count % 500 === 0) log.info("CDN_SYNC", `Uploaded ${count} files...`);
      batch = [];
    }
  }

  if (batch.length > 0) {
    await Promise.all(batch);
    count += batch.length;
  }

  return count;
}

async function setDeployedVersion(version: string): Promise<void> {
  await s3.send(
    new PutObjectCommand({
      Bucket: R2_BUCKET_NAME,
      Key: MARKER_KEY,
      Body: version,
      ContentType: "text/plain",
    }),
  );
}

// ── Main ────────────────────────────────────────────────────────

export async function syncCdn(): Promise<void> {
  const startTime = Date.now();

  log.info("CDN_SYNC", "Checking for Data Dragon updates...");

  const latest = await getLatestVersion();
  const deployed = await getCurrentDeployedVersion();

  log.info("CDN_SYNC", `Latest: ${latest}, Deployed: ${deployed ?? "(none)"}`);

  if (latest === deployed) {
    log.info("CDN_SYNC", "CDN is up to date, nothing to do");
    return;
  }

  log.info("CDN_SYNC", `New version detected! Updating ${deployed ?? "nothing"} → ${latest}`);

  const workDir = join(tmpdir(), `dragontail-${latest}-${Date.now()}`);
  const tgzPath = join(workDir, `dragontail-${latest}.tgz`);
  const extractDir = join(workDir, "extracted");

  try {
    await mkdir(workDir, { recursive: true });

    // 1. Download
    await downloadTgz(DRAGONTAIL_URL(latest), tgzPath);

    // 2. Extract
    await extractTgz(tgzPath, extractDir);

    // 3. Upload to R2
    const fileCount = await uploadToR2(extractDir, latest);
    log.info("CDN_SYNC", `Uploaded ${fileCount} files to R2`);

    // 4. Update marker
    await setDeployedVersion(latest);
    log.info("CDN_SYNC", `Marker updated to ${latest}`);

    const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    log.info("CDN_SYNC", `CDN sync completed in ${elapsed}min`);
  } finally {
    // Cleanup temp files
    await rm(workDir, { recursive: true, force: true }).catch(() => {});
  }
}

// ── Run standalone ──────────────────────────────────────────────
if (import.meta.main) {
  syncCdn()
    .then(() => process.exit(0))
    .catch((err) => {
      const msg = err instanceof Error ? err.message : String(err);
      log.error("CDN_SYNC", `Fatal error: ${msg}`);
      process.exit(1);
    });
}
