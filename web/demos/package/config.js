var config = {};
config.web = {};
config.gateway = {};
config.machine = {};
config.machine.redis = {};
config.fraud = {};
config.fraud.mongo = {};
config.adsdimensions = {};
config.adsdimensions.redis = {};

config.web.port = process.env.PORT || 3003;
config.web.staticDir = process.env.STATIC_DIR || '/app';
config.gateway.host = process.env.GATEWAY_HOST || 'localhost';
config.gateway.port = process.env.GATEWAY_PORT || 9090;
config.machine.redis.host = process.env.MACHINE_REDIS_HOST || null;
config.machine.redis.port = process.env.MACHINE_REDIS_PORT || 6379;
config.machine.redis.dbIndex = process.env.MACHINE_REDIS_DB_INDEX || 2;
config.adsdimensions.redis.host = process.env.ADS_REDIS_HOST || null;
config.adsdimensions.redis.port = process.env.ADS_REDIS_PORT || 6379;
config.adsdimensions.redis.dbIndex = process.env.ADS_REDIS_DB_INDEX || 0;
config.fraud.mongo.host = process.env.MONGODB_HOST || null;
config.fraud.mongo.port = process.env.MONGODB_PORT || 27017;
config.fraud.mongo.dbName = 'frauddetect';

// client settings (passed to the browser)
config.settings = {};
var settings = config.settings;

settings.twitter = {};
settings.mobile = {};
settings.mobile.topic = {};
settings.machine = {};
settings.machine.range = {};
settings.dimensions = {};
settings.dimensions.range = {};
settings.fraud = {};

settings.webSocketURL = 'ws://' + config.gateway.host + ':' + config.gateway.port + '/pubsub';
settings.appsURL = 'http://' + config.gateway.host + ':' + config.gateway.port + '/static/#ops/apps/';

settings.twitter.appName = 'TwitterCustomerApplication';
settings.twitter.topic = 'demos.twitter.topURLs';
settings.mobile.topic.out = 'demos.mobile.phoneLocationQueryResult';
settings.mobile.topic.in = 'demos.mobile.phoneLocationQuery';
settings.mobile.appName = 'MobileLocatorCustomerApplication';
settings.machine.appName = 'MachineDataCustomerApplication';
settings.machine.lookback = 180; // default lookback (minutes)
settings.machine.metricformat = '#.0';
settings.machine.pollInterval = 1000; // milliseconds
settings.machine.range.customer = { start: 1, stop: 10 };
settings.machine.range.product = { start: 4, stop: 6 };
settings.machine.range.os = { start: 10, stop: 12 };
settings.machine.range.software1 = { start: 10, stop: 12 };
settings.machine.range.software2 = { start: 12, stop: 14 };
settings.machine.range.deviceId = { start: 1, stop: 50 };
settings.dimensions.appName = 'AdsDimensionCustomerApplication';
settings.dimensions.lookback = 180; // default lookback (minutes)
settings.dimensions.pollInterval = 1; // seconds
settings.dimensions.range.publisher = { start: 0, stop: 49 };
settings.dimensions.range.advertiser = { start: 0, stop: 99 };
settings.dimensions.range.adunit = { start: 0, stop: 4 };
settings.fraud.appName = 'FraudDetectionCustomerApplication';

module.exports = config
