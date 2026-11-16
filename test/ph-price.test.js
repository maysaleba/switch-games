const test = require('node:test');
const assert = require('node:assert/strict');
const { formatPriceRow, isCurrentlyDiscounted } = require('../price_ph');

test('PH price normalization matches the shared price-cache shape', () => {
  const price = formatPriceRow({
    regular_price: { raw_value: '100.00', currency: 'PHP' },
    discount_price: {
      raw_value: '75.00',
      currency: 'PHP',
      start_datetime: '2026-11-01T00:00:00Z',
      end_datetime: '2026-11-20T00:00:00Z',
    },
  });

  assert.deepEqual(price, {
    regular: '100.00',
    regular_currency: 'PHP',
    sale: '75.00',
    sale_currency: 'PHP',
    sale_start: '2026-11-01T00:00:00Z',
    sale_end: '2026-11-20T00:00:00Z',
  });
});

test('only a real discount active now is selected for PH merge', () => {
  const now = new Date('2026-11-16T12:00:00Z');
  const base = {
    regular: '100',
    sale: '75',
    sale_start: '2026-11-01T00:00:00Z',
    sale_end: '2026-11-20T00:00:00Z',
  };

  assert.equal(isCurrentlyDiscounted(base, now), true);
  assert.equal(isCurrentlyDiscounted({ ...base, sale: '100' }, now), false);
  assert.equal(isCurrentlyDiscounted({ ...base, sale: null }, now), false);
  assert.equal(isCurrentlyDiscounted({ ...base, sale_start: '2026-11-17T00:00:00Z' }, now), false);
  assert.equal(isCurrentlyDiscounted({ ...base, sale_end: '2026-11-16T12:00:00Z' }, now), false);
});
