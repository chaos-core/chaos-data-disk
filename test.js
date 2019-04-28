const assert = require('assert');
const Path = require('path');
const {from, of} = require('rxjs');
const {flatMap, tap, toArray} = require('rxjs/operators');

const ChaosDataDisk = require('./lib/chaos-data-disk');

const chaos = {
  logger: {
    debug: (msg) => {console.debug("chaos-logger:", msg)}
  }
};

const config = {
  type: 'disk',
  dataDir: Path.join(__dirname, 'data'),
};

const dataDisk = new ChaosDataDisk(chaos, config);

function testSave(type, id, keyword, data) {
  return of('').pipe(
    tap(() => console.log(`saving\t${type}:${id}:${keyword}\t=>\t${data}`)),
    flatMap(() => dataDisk.setData(type, id, keyword, data)),
    tap((saved) => console.log(`saved\t${type}:${id}:${keyword}\t=>\t${saved}`)),
    tap((saved) => assert.deepStrictEqual(saved, data)),
  )
}

function testRead(type, id, keyword, expected) {
  return of('').pipe(
    tap(() => console.log(`Read\t${type}:${id}:${keyword}`)),
    flatMap(() => dataDisk.getData(type, id, keyword)),
    tap((saved) => console.log(`Return\t${type}:${id}:${keyword}\t=>\t${saved}`)),
    tap((saved) => assert.deepStrictEqual(saved, expected)),
  )
}

from([
  ["guild", "guildId1", "keyword.string", "string"],
  ["guild", "guildId1", "keyword.number", 10],
  ["guild", "guildId1", "keyword.json", {json: true}],
  ["guild", "guildId1", "keyword.null", null],
]).pipe(
  flatMap(([type, id, keyword, data]) => of('').pipe(
    flatMap(() => testSave(type, id, keyword, data)),
    flatMap(() => testRead(type, id, keyword, data)),
  )),
  toArray(),
  flatMap(() => testRead("guild", "guildId1", "keyword.unsaved", undefined)),
).subscribe();