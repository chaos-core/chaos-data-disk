const fs = require('fs');
const Path = require('path');

const Observable = require('rxjs').Observable;
const Subject = require('rxjs').Subject;

const ReadFile$ = Observable.bindNodeCallback(fs.readFile);
const WriteFile$ = Observable.bindNodeCallback(fs.writeFile);

class NixDataDisk {
  /**
   * @param nix
   * @param config
   * @param config.dataDir
   */
  constructor(nix, config) {
    this.nix = nix;
    this.type = 'Disk';
    this._dataDir = config.dataDir;

    if (!fs.existsSync(this._dataDir)) {
      fs.mkdirSync(this._dataDir);
    }

    this._writer$ =
      new Subject()
        .groupBy((change) => change.filename)
        .flatMap((group$) => group$.concatMap((change) => this._saveChange(change)))
        .share();

    this._writer$.subscribe();
  }

  onNixListen() {
    this.nix.shutdown$.subscribe(() => this._writer$.complete());
    return Observable.of(true);
  }

  /**
   * @param type
   * @param keyword
   * @param id
   *
   * @return {Observable}
   */
  getData(type, id, keyword) {
    let filename = this._getFilename(type, id);
    return this._readFile(filename)
      .map((data) => data[keyword]);
  }

  /**
   * @param type
   * @param id
   * @param keyword
   * @param value
   *
   * @return {Observable}
   */
  setData(type, id, keyword, value) {
    return Observable.of({type, id, keyword, value})
      .flatMap((change) => {
        change.filename = this._getFilename(change.type, change.id);
        this._writer$.next(change);
        return this._writer$
          .filter((saved) => Object.is(saved, change))
          .take(1);
      })
      .map((change) => change.savedValue);
  }

  /**
   * @param type
   * @param id
   *
   * @private
   *
   * @return {String}
   */
  _getFilename(type, id) {
    let folder = Path.join(this._dataDir, type);
    let filename = Path.join(folder, id + '.json');

    if (!fs.existsSync(Path.join(folder))) {
      fs.mkdirSync(Path.join(folder));
    }

    return filename;
  }

  /**
   * @param filename
   *
   * @private
   *
   * @return {Observable<Object>}
   */
  _readFile(filename) {
    return ReadFile$(filename)
      .catch((err) => {
        if (err.code === 'ENOENT') { // ENOENT => Error No Entity
          return Observable.of('{}');
        } else {
          throw err;
        }
      })
      .map((contents) => JSON.parse(contents));
  }

  /**
   * @param filename
   * @param data
   *
   * @private
   *
   * @returns {Observable<Object>}
   */
  _saveFile(filename, data) {
    return WriteFile$(filename, JSON.stringify(data, null, '  '));
  }

  /**
   * @param change.keyword
   * @param change.filename
   * @param change.value
   *
   * @private
   *
   * @returns {Observable<Object>}
   */
  _saveChange(change) {
    return Observable.of('')
      .do(() => this.nix.logger.debug(`nix-data-disk write START: ${change.filename} => ${change.keyword}: ${change.value}`))
      .flatMap(() => this._readFile(change.filename))
      .map((data) => {
        let newData = {...data};
        newData[change.keyword] = change.value;
        return newData;
      })
      .flatMap((data) => this._saveFile(change.filename, data))
      .flatMap(() => this._readFile(change.filename))
      .map((savedData) => {
        change.savedValue = savedData[change.keyword];
        return change;
      })
      .do((change) => this.nix.logger.debug(`nix-data-disk write END: ${change.filename} => ${change.keyword}: ${change.savedValue}`));
  }
}

module.exports = NixDataDisk;
