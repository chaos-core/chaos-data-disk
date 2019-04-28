const fs = require('fs');
const Path = require('path');
const {bindNodeCallback, Subject, of} = require('rxjs');
const {concatMap, tap, flatMap, catchError, mapTo, map} = require('rxjs/operators');

const ReadFile$ = bindNodeCallback(fs.readFile);
const WriteFile$ = bindNodeCallback(fs.writeFile);

class ChaosDataDisk {
  /**
   * @param chaos {ChaosCore}
   * @param config
   * @param config.dataDir
   */
  constructor(chaos, config) {
    this.chaos = chaos;
    this.type = "Disk";
    this._dataDir = config.dataDir;

    if (!fs.existsSync(this._dataDir)) {
      fs.mkdirSync(this._dataDir);
    }

    this._writeQueue$ = new Subject();
    this._writeQueue$.pipe(
      concatMap((data) => of(data).pipe(
        tap((data) => this.chaos.logger.debug(`Write START: ${data.filename} => ${data.keyword} = ${data.value}`)),
        flatMap((data) => this._readFromFile(data.filename).pipe(
          tap((fileData) => fileData[data.keyword] = data.value),
          flatMap((fileData) => this._saveToFile(data.filename, fileData)),
          tap((saved) => data.savedValue = saved[data.keyword]),
          mapTo(data),
        )),
        tap((data) => this.chaos.logger.debug(`Write END: ${data.filename} => ${data.keyword} = ${data.value}`)),
        tap((data) => data.callback(data.savedValue)),
      )),
    ).subscribe();
  }

  onListen() {
    this.chaos.shutdown$.subscribe(() => this._writeQueue$.complete());
  }

  /**
   *
   * @param type
   * @param keyword
   * @param id
   *
   * @return {Observable}
   */
  getData(type, id, keyword) {
    let filename = this._getDataFile(type, id);
    return this._readFromFile(filename).pipe(
      map((data) => data[keyword]),
    );
  }

  /**
   *
   * @param type
   * @param id
   * @param keyword
   * @param value
   *
   * @return {Observable}
   */
  setData(type, id, keyword, value) {
    let filename = this._getDataFile(type, id);
    let saved$ = new Subject();

    this._writeQueue$.next({
      filename,
      keyword,
      value,
      callback: (newValue) => {
        saved$.next(newValue);
        saved$.complete();
      },
    });

    return saved$;
  }

  /**
   *
   * @private
   *
   * @param type
   * @param id
   *
   * @return {String}
   */
  _getDataFile(type, id) {
    let folder = Path.join(this._dataDir, type);
    let filename = Path.join(folder, id + ".json");

    if (!fs.existsSync(Path.join(folder))) {
      fs.mkdirSync(Path.join(folder));
    }

    return filename;
  }

  /**
   *
   * @private
   *
   * @param filename
   *
   * @return {Observable}
   */
  _readFromFile(filename) {
    return of(filename).pipe(
      flatMap((filename) => ReadFile$(filename)),
      map((contents) => JSON.parse(contents)),
      catchError((err) => {
        if (err.code === 'ENOENT') { // ENOENT => Error No Entity
          return WriteFile$(filename, "{}").pipe(
            flatMap(() => this._readFromFile(filename)),
          );
        } else {
          throw err;
        }
      }),
    );
  }

  /**
   *
   * @private
   *
   * @param filename
   * @param data
   *
   * @return {Observable}
   */
  _saveToFile(filename, data) {
    return of(data).pipe(
      map((data) => JSON.stringify(data, null, '  ')),
      flatMap((dataString) => WriteFile$(filename, dataString)),
      flatMap(() => this._readFromFile(filename)),
    );
  }
}

module.exports = ChaosDataDisk;