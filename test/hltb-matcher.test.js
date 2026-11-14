const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { createHltbMatcher } = require('../hltb-matcher');

const empty = { MainStory: '', MainExtra: '', Completionist: '', LowestPrice: '' };

test('prefers an edition entry, then tries the base title', () => {
  const match = createHltbMatcher([
    { game_id: 1, game_name: 'Example Game', comp_main: 5400, comp_plus: 9000 },
    { game_id: 2, game_name: 'Example Game: Complete Edition', comp_main: 14400 },
    { game_id: 3, game_name: 'Ultimate Chicken Horse', comp_main: 3600 },
  ]);
  assert.equal(match('Example Game: Complete Edition').LowestPrice, '/game/2');
  assert.deepEqual(match('Example Game – Digital Deluxe Edition'), {
    MainStory: 2, MainExtra: 3, Completionist: '', LowestPrice: '/game/1',
  });
  assert.equal(match('Example Game (Ultimate Edition)').LowestPrice, '/game/1');
  assert.equal(match('Example Game™ - Deluxe Edition - Nintendo Switch™ 2 Edition').LowestPrice, '/game/1');
  assert.equal(match('Ultimate Chicken Horse').LowestPrice, '/game/3');
  assert.deepEqual(match('Example Game 2 Deluxe Edition'), empty);
  assert.deepEqual(match('Example Game and Other Game Bundle'), empty);
  assert.deepEqual(match('Example Game: Directors Cut'), empty);
  assert.deepEqual(createHltbMatcher(null)('Example Game'), empty);
});

test('preserves Switch platform handling, special titles and similarity threshold', () => {
  const match = createHltbMatcher([
    { game_id: 4, game_name: 'Example Game', comp_main: 3600 },
    { game_id: 5, game_name: 'CRISIS CORE: FINAL FANTASY VII REUNION' },
    { game_id: 6, game_name: 'Prince of Persia: The Lost Crown' },
    { game_id: 7, game_name: 'one two three four five six seven eight nine ten' },
  ]);
  assert.equal(match('Example Game - Nintendo Switch™ 2 Edition').LowestPrice, '/game/4');
  assert.equal(match('CRISIS CORE –FINAL FANTASY VII– REUNION').LowestPrice, '/game/5');
  assert.equal(match('Prince of Persia The Lost Crown').LowestPrice, '/game/6');
  assert.equal(match('one two three four five six seven eight nine ten eleven').LowestPrice, '/game/7');
  assert.deepEqual(match('one two three four five six seven eight nine ten eleven twelve'), empty);
});

test('sync refreshes HLTB fields for new and unchanged rows on every run', (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'switch-hltb-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  fs.mkdirSync(path.join(dir, 'output'));
  const write = (name, value) => fs.writeFileSync(path.join(dir, name), JSON.stringify(value));
  write('output/merged_enriched.json', [{ title: 'Example Game Deluxe Edition', urlKey: 'example-switch', active_in_base: true }]);
  write('output/merged_enriched_with_prices.json', [{ key: 'example-switch', prices: { US: { sale: '10', regular: '20', sale_end: '2026-12-01' } } }]);
  write('remote.json', []);
  const hltb = [{ game_id: 5, game_name: 'Example Game', comp_main: 3600, comp_plus: 7200, comp_100: 10800 }];
  const preload = path.join(dir, 'mock-https.js');
  fs.writeFileSync(preload, `const https = require('https');
const fs = require('fs');
const { EventEmitter } = require('events');
https.get = (_url, callback) => {
  const req = new EventEmitter();
  process.nextTick(() => {
    const res = new EventEmitter();
    res.statusCode = 200;
    callback(res);
    res.emit('data', fs.readFileSync('remote.json', 'utf8'));
    res.emit('end');
  });
  return req;
};`);
  const run = () => {
    const result = spawnSync(process.execPath, ['--require', preload, path.join(__dirname, '..', 'sync_build_csvjson.js')], { cwd: dir, encoding: 'utf8' });
    assert.equal(result.status, 0, result.stderr);
    return { row: JSON.parse(fs.readFileSync(path.join(dir, 'csvjson.json'), 'utf8'))[0], log: result.stdout };
  };
  write('hltb.json', hltb);
  let { row } = run();
  assert.deepEqual([row.LowestPrice, row.MainStory, row.MainExtra, row.Completionist], ['/game/5', 1, 2, 3]);
  write('remote.json', [row]);
  hltb[0].game_id = 6;
  hltb[0].comp_main = 0;
  hltb[0].comp_plus = 14400;
  write('hltb.json', hltb);
  const updated = run();
  row = updated.row;
  assert.match(updated.log, /kept:\s+1/);
  assert.deepEqual([row.LowestPrice, row.MainStory, row.MainExtra, row.Completionist], ['/game/6', '', 4, 3]);
  write('remote.json', [row]);
  write('hltb.json', []);
  row = run().row;
  assert.deepEqual([row.LowestPrice, row.MainStory, row.MainExtra, row.Completionist], ['', '', '', '']);
});