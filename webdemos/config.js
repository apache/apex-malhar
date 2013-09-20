var path = require('path');

var config = {};
config.web = {};
config.redis = {};
config.adsdimensions = {};
config.adsdimensions.redis = {};
config.siteops = {};
config.siteops.redis = {};

config.web.port = process.env.PORT || 3000;
config.web.webSocketUrl = null; // 'ws://localhost:9090/pubsub' for local Daemon
config.adsdimensions.redis.host = null; // 'localhost' for local Redis server
config.adsdimensions.redis.port = null; // 6379 is default Redis port
config.siteops.redis.host = null; // 'localhost' for local Redis server
config.siteops.redis.port = null; // 6379 is default Redis port

module.exports = config