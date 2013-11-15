var config = {};
config.web = {};
config.daemon = {};
config.machine = {};
config.machine.redis = {};
config.fraud = {};
config.fraud.mongo = {};
config.adsdimensions = {};
config.adsdimensions.redis = {};

config.web.port = process.env.PORT || 3003;
config.daemon.host = 'localhost';
config.daemon.port = 9090;
config.machine.redis.host = 'localhost';
config.machine.redis.port = 6379;
config.machine.redis.dbIndex = 2;
//config.machine.forcedDelay = 5000; // for testing purpose only
config.adsdimensions.redis.host = 'localhost';
config.adsdimensions.redis.port = 6379;
config.fraud.mongo.host = 'localhost';
config.fraud.mongo.port = 27017;

module.exports = config
