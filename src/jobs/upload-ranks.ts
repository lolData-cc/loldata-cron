import {
  S3Client,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { log } from "../logger";
import {
  R2_ACCOUNT_ID,
  R2_ACCESS_KEY_ID,
  R2_SECRET_ACCESS_KEY,
  R2_BUCKET_NAME,
} from "../config";
import { mkdir } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";

const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
});

const TIERS = [
  "iron",
  "bronze",
  "silver",
  "gold",
  "platinum",
  "emerald",
  "diamond",
  "master",
  "grandmaster",
  "challenger",
];

// Community Dragon ranked emblem URLs
const EMBLEM_URL = (tier: string) =>
  `https://raw.communitydragon.org/latest/plugins/rcp-fe-lol-shared-components/global/default/images/${tier}.png`;

async function downloadImage(url: string): Promise<Buffer> {
  const res = await fetch(url, { signal: AbortSignal.timeout(30_000) });
  if (!res.ok) throw new Error(`Failed to download ${url}: ${res.status}`);
  return Buffer.from(await res.arrayBuffer());
}

async function uploadToR2(key: string, body: Buffer): Promise<void> {
  await s3.send(
    new PutObjectCommand({
      Bucket: R2_BUCKET_NAME,
      Key: key,
      Body: body,
      ContentType: "image/png",
      CacheControl: "public, max-age=31536000, immutable",
    }),
  );
}

async function main() {
  log.info("RANK_ICONS", "Downloading and uploading rank icons to R2...");

  // Get latest version for versioned paths
  const versions = await fetch("https://ddragon.leagueoflegends.com/api/versions.json")
    .then(r => r.json()) as string[];
  const latest = versions[0];
  log.info("RANK_ICONS", `Latest patch: ${latest}`);

  for (const tier of TIERS) {
    try {
      const url = EMBLEM_URL(tier);
      log.info("RANK_ICONS", `Downloading ${tier}...`);
      const img = await downloadImage(url);

      // Upload to all paths the frontend might use
      const paths = [
        `ranks/${tier}.png`,
        `img/miniranks/${tier}.png`,
        `${latest}/ranks/${tier}.png`,
        `${latest}/img/miniranks/${tier}.png`,
      ];
      for (const p of paths) {
        await uploadToR2(p, img);
      }
      log.info("RANK_ICONS", `Uploaded ${tier} (${(img.length / 1024).toFixed(0)} KB) to ${paths.length} paths`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      log.error("RANK_ICONS", `Failed for ${tier}: ${msg}`);
    }
  }

  log.info("RANK_ICONS", "Done!");
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    log.error("RANK_ICONS", `Fatal: ${err}`);
    process.exit(1);
  });
