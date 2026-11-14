const SWITCH_SUFFIX = /\s*(?:[-–—:]\s*)?(?:for\s+)?nintendo\s+switch\s*(?:™\s*)?(?:2\s*)?(?:edition)?\s*$/i;
const EDITION_SUFFIX = /(?:\s+|\s*[-–—:]\s*)\(?(?:(?:digital|standard|deluxe|ultimate|definitive|complete|collectors?|gold|premium|special|enhanced|legendary|game of the year|goty)\s+)+edition\)?\s*$/i;

// Keep Switch's existing title normalization and special title corrections.
function normalizeTitle(title) {
  return String(title || '')
    .replace(SWITCH_SUFFIX, '')
    .replace(/:/g, '')
    .replace(/[^a-zA-Z0-9+ ]/g, '')
    .trim()
    .toLowerCase();
}

function createHltbMatcher(rows) {
  const entries = [];
  const exact = new Map();
  const byToken = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    const title = normalizeTitle(row?.game_name);
    if (!title) continue;
    const tokens = new Set(title.split(' ').filter(Boolean));
    const entry = { row, tokens, index: entries.length };
    entries.push(entry);
    if (!exact.has(title)) exact.set(title, row);
    for (const token of tokens) {
      if (!byToken.has(token)) byToken.set(token, []);
      byToken.get(token).push(entry);
    }
  }

  function find(title) {
    if (!title) return null;
    if (exact.has(title)) return exact.get(title);
    const tokens = new Set(title.split(' ').filter(Boolean));
    const candidates = new Set();
    for (const token of tokens) {
      for (const entry of byToken.get(token) || []) candidates.add(entry);
    }
    let best = null;
    let bestScore = 0;
    // Match the existing Switch behavior for ties: first HLTB row wins.
    for (const entry of [...candidates].sort((a, b) => a.index - b.index)) {
      let intersection = 0;
      for (const token of tokens) if (entry.tokens.has(token)) intersection++;
      const score = intersection / (tokens.size + entry.tokens.size - intersection);
      if (score > bestScore) {
        best = entry.row;
        bestScore = score;
        if (score === 1) break;
      }
    }
    return bestScore >= 0.9 ? best : null;
  }

  return (value) => {
    const title = String(value || '')
      .replace('CRISIS CORE –FINAL FANTASY VII– REUNION', 'CRISIS CORE: FINAL FANTASY VII REUNION')
      .replace('Prince of Persia The Lost Crown', 'Prince of Persia: The Lost Crown');
    const withoutPlatform = title.replace(SWITCH_SUFFIX, '').replace(/[™®©]/g, '');
    const baseTitle = withoutPlatform.replace(EDITION_SUFFIX, '');
    const match = find(normalizeTitle(title)) ||
      (baseTitle !== withoutPlatform ? find(normalizeTitle(baseTitle)) : null);
    return {
      MainStory: Math.round((Number(match?.comp_main) || 0) / 3600) || '',
      MainExtra: Math.round((Number(match?.comp_plus) || 0) / 3600) || '',
      Completionist: Math.round((Number(match?.comp_100) || 0) / 3600) || '',
      LowestPrice: match?.game_id ? `/game/${match.game_id}` : '',
    };
  };
}

module.exports = { createHltbMatcher };