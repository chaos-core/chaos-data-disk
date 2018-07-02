const fs = require('fs');
const Path = require('path');
const Observable = require('rxjs').Observable;
const Subject = require('rxjs').Subject;

const ReadFile$ = Observable.bindNodeCallback(fs.readFile);
const WriteFile$ = Observable.bindNodeCallback(fs.writeFile);

class NixDataDisk {
  /**
   *
   * @param config
   * @param config.dataDir
   */
  constructor(nix, config) {
    this.nix = nix;
    this.type = "Disk";
    this._dataDir = config.dataDir;

    if (!fs.existsSync(this._dataDir)) {
      fs.mkdirSync(this._dataDir);
    }

    this._writeQueue$ = new Subject();
    this._writer$ = this._writeQueue$.controlled();
    this._writer$.request(1);
    this._writer$
      .do((data) => this.nix.logger.debug(`Write START: ${data.filename} => ${data.keyword} = ${data.value}`))
      .flatMap((data) => {
        return this._readFromFile(data.filename)
          .flatMap((fileData) => {
            fileData[data.keyword] = data.value;
            return this._saveToFile(data.filename, fileData);
          })
          .do((saved) => data.savedValue = saved[data.keyword])
          .map(data);
      })
      .do((data) => this.nix.logger.debug(`Write END: ${data.filename} => ${data.keyword} = ${data.value}`))
      .do((data) => data.callback(data.savedValue))
      .subscribe(() => this._writer$.request(1));
  }

  onNixListen() {
    this.nix.shutdown$.subscribe(() => this._writeQueue$.onCompleted());
    return Observable.of(true);
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
    return this._readFromFile(filename)
      .map((data) => data[keyword]);
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

    this._writeQueue$.onNext({
      filename,
      keyword,
      value,
      callback: (newValue) => {
        saved$.onNext(newValue);
        saved$.onCompleted();
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
    return ObservableSubject.of(filename) //Don't read the file right away, wait till something subscribes
      .flatMap((filename) => ReadFile$(filename))
      .map((contents) => JSON.parse(contents))
      .catch((err) => {
        if (err.code === 'ENOENT') { // ENOENT => Error No Entity
          return WriteFile$(filename, "{}")
            .flatMap(() => this._readFromFile(filename));
        } else {
          throw err;
        }
      });
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
    return Observable.of(data)
      .map((data) => JSON.stringify(data, null, '  '))
      .flatMap((dataString) => WriteFile$(filename, dataString))
      .flatMap(() => this._readFromFile(filename));
  }
}

module.exports = NixDataDisk;
