var config = {};
config.web = {};
config.daemon = {};
config.machine = {};
config.machine.redis = {};
config.fraud = {};
config.fraud.mongo = {};

config.web.port = process.env.PORT || 3003;
config.daemon.host = 'localhost';
config.daemon.port = 3490;
config.machine.redis.host = 'localhost';
config.machine.redis.port = 8379;
config.machine.redis.dbIndex = 2;

module.exports = config