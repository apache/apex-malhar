var path = require('path');

var config = {};
config.web = {};
config.redis = {};

config.web.port = process.env.PORT || 3000;
config.web.webSocketUrl = null; // 'ws://localhost:9090/pubsub' for local Daemon
config.redis.host = null; // 'localhost' for local Redis server
config.redis.port = null; // 6379 is default Redis port

module.exports = config