// src/lib/items.ts
//
// Data Dragon item classification for the ingest: which item ids count as
// "legendaries" (for the per-participant build ORDER we store on participants)
// and which are boots. Mirrors the Explorer's completed-item pool (itemData.ts
// in the backend) so the build-slot semantics stay consistent — but:
//   • legendaries here EXCLUDE boots (boots are stored separately) and use a
//     2000g floor so only real legendaries land in the order, and
//   • Dark Seal (2010) + Mejai's (3041) are force-included (snowball items the
//     product wants tracked even though they're cheap).
// Loaded once from DDragon and cached, with a tiny fallback so a fetch failure
// doesn't break the ingest (it just stores fewer items).

const VERSIONS_URL = "https://ddragon.leagueoflegends.com/api/versions.json";
const itemJsonUrl = (v: string) =>
  `https://ddragon.leagueoflegends.com/cdn/${v}/data/en_US/item.json`;

const FORCE_LEGENDARY = [2010, 3041]; // Dark Seal, Mejai's Soulstealer

let _legendary: Set<number> | null = null;
let _boots: Set<number> | null = null;
let _loading: Promise<void> | null = null;

async function load(): Promise<void> {
  const legendary = new Set<number>(FORCE_LEGENDARY);
  const boots = new Set<number>();
  try {
    const vRes = await fetch(VERSIONS_URL, { signal: AbortSignal.timeout(10_000) });
    const versions = (await vRes.json()) as string[];
    const version = versions?.[0];
    if (!version) throw new Error("no version");

    const iRes = await fetch(itemJsonUrl(version), { signal: AbortSignal.timeout(15_000) });
    const json = (await iRes.json()) as { data: Record<string, any> };
    const data = json?.data ?? {};

    for (const idStr of Object.keys(data)) {
      const id = Number(idStr);
      const it = data[idStr];
      if (!Number.isFinite(id) || !it?.name) continue;

      const onSR = it?.maps?.["11"] === true;
      const purchasable = it?.gold?.purchasable === true;
      const total = Number(it?.gold?.total) || 0;
      const isComponent = Array.isArray(it?.into) && it.into.length > 0;
      const consumable = it?.consumed === true || it?.consumeOnFull === true;
      const tags: string[] = Array.isArray(it?.tags) ? it.tags : [];
      const trinket = tags.includes("Trinket");
      const isBoots = tags.includes("Boots");

      if (!onSR || !purchasable || consumable || trinket) continue;
      if (isBoots) {
        if (total >= 600) boots.add(id); // tier-2+ boots (skip the 300g tier-1)
        continue;
      }
      // Legendaries: a finished item (no `into`) costing 2000g+. Excludes boots,
      // components, cheap finished starters. Dark Seal/Mejai added via FORCE.
      if (!isComponent && total >= 2000) legendary.add(id);
    }
    _legendary = legendary;
    _boots = boots;
    console.log(`[items] ${legendary.size} legendaries, ${boots.size} boots (v${version})`);
  } catch (e) {
    _legendary = legendary; // at least Dark Seal / Mejai
    _boots = boots;
    console.warn("[items] DDragon item fetch failed:", (e as Error)?.message ?? e);
  }
}

/** Kick off the load (idempotent). Call once before ingesting a batch. */
export async function warmItems(): Promise<void> {
  if (_legendary) return;
  if (!_loading) _loading = load();
  await _loading;
}

export function legendaryPool(): Set<number> {
  return _legendary ?? new Set<number>(FORCE_LEGENDARY);
}
export function bootsPool(): Set<number> {
  return _boots ?? new Set<number>();
}
