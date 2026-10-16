const test = require('node:test');
const assert = require('node:assert/strict');
const { resolvePlaceholders, normalizeDashboardPipeline, buildPipeline } = require('../pipeline');

test('resolvePlaceholders replaces time placeholders at any depth', () => {
  const pipeline = [{ $match: { 'search_metadata.search_timestamp': { $gte: '$$ISO_FROM', $lte: '$$ISO_TO' } } }];
  const out = resolvePlaceholders(pipeline, '2026-01-01T00:00:00.000Z', '2026-01-02T00:00:00.000Z');
  assert.equal(out[0].$match['search_metadata.search_timestamp'].$gte, '2026-01-01T00:00:00.000Z');
  assert.equal(out[0].$match['search_metadata.search_timestamp'].$lte, '2026-01-02T00:00:00.000Z');
});

test('buildPipeline converts Grafana timestamps in milliseconds to ISO strings', () => {
  const pipeline = [{ $match: { t: { $gte: '$$ISO_FROM' } } }];
  const out = buildPipeline({ pipeline, timestamp_from: 1767225600000, timestamp_to: null });
  assert.equal(out[0].$match.t.$gte, '2026-01-01T00:00:00.000Z');
  assert.equal(Object.hasOwn(out[0].$match.t, '$lte'), false);
});

test('normalizeDashboardPipeline maps the "All" wildcard to the country field', () => {
  const out = normalizeDashboardPipeline([{ $group: { _id: '.*', count: { $sum: 1 } } }]);
  assert.equal(out[0].$group._id, '$country_code');
  assert.equal(out.length, 1);
});

test('normalizeDashboardPipeline expands a multi-value variable into a $match stage', () => {
  const out = normalizeDashboardPipeline([{ $group: { _id: '{EC,ES}', count: { $sum: 1 } } }]);
  assert.deepEqual(out[0], { $match: { country_code: { $in: ['EC', 'ES'] } } });
  assert.equal(out[1].$group._id, '$country_code');
});

test('normalizeDashboardPipeline maps nested wildcards to real field paths', () => {
  const out = normalizeDashboardPipeline([
    { $group: { _id: { keyword: '.*', country: '.*', category: '.*' }, count: { $sum: 1 } } },
  ]);
  assert.deepEqual(out[0].$group._id, {
    keyword: '$search_metadata.search_keyword',
    country: '$country_code',
    category: '$domain_category',
  });
});

test('normalizeDashboardPipeline does not mutate the incoming stages', () => {
  const input = [{ $group: { _id: '.*', count: { $sum: 1 } } }];
  normalizeDashboardPipeline(input);
  assert.equal(input[0].$group._id, '.*');
});

test('buildPipeline prepends the domain_category filter', () => {
  const out = buildPipeline({
    pipeline: [{ $group: { _id: '$domain', count: { $sum: 1 } } }],
    domain_category: ['hotel', 'airline'],
  });
  assert.deepEqual(out[0], { $match: { domain_category: { $in: ['hotel', 'airline'] } } });
  assert.equal(out[1].$group._id, '$domain');
});