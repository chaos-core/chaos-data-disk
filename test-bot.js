const ChaosCore = require('chaos-core');
const config = require('./config');

config.dataSource.type = 'disk';

const testBot = new ChaosCore(config);
testBot.listen();
