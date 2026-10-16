/**
 * Pure helpers for shaping Grafana-driven aggregation pipelines.
 * Kept free of I/O so they can be unit tested without a running MongoDB.
 */

const FIELD_MAP = {
  country: '$country_code',
  country_code: '$country_code',
  keyword: '$search_metadata.search_keyword',
  domain_category: '$domain_category',
  category: '$domain_category',
};

/** Replaces $$ISO_FROM / $$ISO_TO placeholders with the dashboard time range. */
function resolvePlaceholders(node, isoFrom, isoTo) {
  if (Array.isArray(node)) {
    return node.map((item) => resolvePlaceholders(item, isoFrom, isoTo));
  }
  if (node && typeof node === 'object') {
    const out = {};
    for (const [key, value] of Object.entries(node)) {
      if (value === '$$ISO_FROM') out[key] = isoFrom;
      else if (value === '$$ISO_TO') out[key] = isoTo;
      else out[key] = resolvePlaceholders(value, isoFrom, isoTo);
    }
    return out;
  }
  return node;
}

/**
 * Grafana sends variable wildcards (".*") or multi-value strings ("{EC,ES}") instead of
 * field paths. Normalise them into real field references and matching $match stages.
 * Returns a new array; the input stages are not mutated.
 */
function normalizeDashboardPipeline(pipeline) {
  const extraMatches = [];
  const stages = pipeline.map((stage) => {
    if (!stage || !stage.$group || !stage.$group._id) return stage;
    const groupId = stage.$group._id;

    if (groupId === '.*') {
      return { ...stage, $group: { ...stage.$group, _id: '$country_code' } };
    }

    if (typeof groupId === 'string' && /^\{.+\}$/.test(groupId)) {
      const values = groupId.slice(1, -1).split(',').map((v) => v.trim()).filter(Boolean);
      extraMatches.push({ $match: { country_code: { $in: values } } });
      return { ...stage, $group: { ...stage.$group, _id: '$country_code' } };
    }

    if (typeof groupId === 'object' && groupId !== null) {
      const newId = {};
      for (const [key, value] of Object.entries(groupId)) {
        const isWildcard = value === '.*';
        const isMultiValue = typeof value === 'string' && /^\{.+\}$/.test(value);
        newId[key] = isWildcard || isMultiValue ? FIELD_MAP[key] || `$${key}` : value;
      }
      return { ...stage, $group: { ...stage.$group, _id: newId } };
    }

    return stage;
  });

  return [...extraMatches, ...stages];
}

/** Builds the final pipeline: time placeholders, dashboard variables, optional category filter. */
function buildPipeline({ pipeline, timestamp_from, timestamp_to, domain_category }) {
  const isoFrom = timestamp_from ? new Date(parseInt(timestamp_from, 10)).toISOString() : null;
  const isoTo = timestamp_to ? new Date(parseInt(timestamp_to, 10)).toISOString() : null;

  let stages = normalizeDashboardPipeline(resolvePlaceholders(pipeline, isoFrom, isoTo));

  if (domain_category) {
    stages = [
      {
        $match: {
          domain_category: Array.isArray(domain_category) ? { $in: domain_category } : { $in: [domain_category] },
        },
      },
      ...stages,
    ];
  }
  return stages;
}

module.exports = { resolvePlaceholders, normalizeDashboardPipeline, buildPipeline, FIELD_MAP };