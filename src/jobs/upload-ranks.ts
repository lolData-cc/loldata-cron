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

// Modern rank icons (current style) from CommunityDragon
const MODERN_URL = (tier: string) =>
  `https://raw.communitydragon.org/latest/plugins/rcp-fe-lol-shared-components/global/default/images/${tier}.png`;

// Legacy 2019 helmet-style rank icons from GitHub
const LEGACY_TIER_MAP: Record<string, string> = {
  iron: "Emblem_Iron",
  bronze: "Emblem_Bronze",
  silver: "Emblem_Silver",
  gold: "Emblem_Gold",
  platinum: "Emblem_Platinum",
  emerald: "Emblem_Platinum", // Emerald didn't exist in 2019, use Platinum as placeholder
  diamond: "Emblem_Diamond",
  master: "Emblem_Master",
  grandmaster: "Emblem_Grandmaster",
  challenger: "Emblem_Challenger",
};
const LEGACY_URL = (tier: string) =>
  `https://cdn.jsdelivr.net/gh/magisteriis/lol-icons-and-emblems/ranked-emblems/${LEGACY_TIER_MAP[tier]}.png`;

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

  // ── Modern rank icons ──
  log.info("RANK_ICONS", "=== Uploading modern rank icons ===");
  for (const tier of TIERS) {
    try {
      const img = await downloadImage(MODERN_URL(tier));
      const paths = [
        `ranks/${tier}.png`,
        `img/miniranks/${tier}.png`,
        `${latest}/ranks/${tier}.png`,
        `${latest}/img/miniranks/${tier}.png`,
      ];
      for (const p of paths) await uploadToR2(p, img);
      log.info("RANK_ICONS", `✅ ${tier} modern (${(img.length / 1024).toFixed(0)} KB)`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      log.error("RANK_ICONS", `Failed modern ${tier}: ${msg}`);
    }
  }

  // ── Legacy 2019 helmet-style rank icons ──
  log.info("RANK_ICONS", "=== Uploading legacy (2019 helmet) rank icons ===");
  for (const tier of TIERS) {
    try {
      const img = await downloadImage(LEGACY_URL(tier));
      const paths = [
        `ranks-legacy/${tier}.png`,
        `img/miniranks-legacy/${tier}.png`,
        `${latest}/ranks-legacy/${tier}.png`,
        `${latest}/img/miniranks-legacy/${tier}.png`,
      ];
      for (const p of paths) await uploadToR2(p, img);
      log.info("RANK_ICONS", `✅ ${tier} legacy (${(img.length / 1024).toFixed(0)} KB)`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      log.error("RANK_ICONS", `Failed legacy ${tier}: ${msg}`);
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
