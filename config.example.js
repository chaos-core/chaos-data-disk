const Path = require('path');

module.exports = {
  ownerUserId: "ownerUserId",
  loginToken: "loginToken",

  logger: {
    level: 'info',
  },

  dataSource: {
    type: 'disk',
    dataDir: Path.join(__dirname, '../data'),
  },
};
