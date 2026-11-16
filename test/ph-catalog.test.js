const test = require('node:test');
const assert = require('node:assert/strict');
const { isOnSale, mapItem } = require('../fetch_ph');

test('PH search sale_flg identifies current sale listings', () => {
  assert.equal(isOnSale({ sale_flg: '1' }), true);
  assert.equal(isOnSale({ sale_flg: 1 }), true);
  assert.equal(isOnSale({ sale_flg: '0' }), false);
  assert.equal(isOnSale({}), false);
});

test('PH catalog mapping stores sale flag and sale status', () => {
  const item = mapItem({
    nsuid: '70010000000001',
    title: 'Example Game',
    lang: ['en'],
    sale_flg: '1',
  });

  assert.equal(item.ph_on_sale, true);
  assert.equal(item.sale_flg, '1');
  assert.equal(item.active_in_base, true);
});

test('PH catalog mapping retains non-sale items as inactive merge candidates', () => {
  const item = mapItem({
    nsuid: '70010000000002',
    title: 'Another Game',
    lang: ['en'],
    sale_flg: '0',
  });

  assert.ok(item);
  assert.equal(item.ph_on_sale, false);
  assert.equal(item.active_in_base, false);
});
