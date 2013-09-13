var path = require('path');

var config = {};
config.web = {};
config.redis = {};

config.web.port = process.env.PORT || 3000;
config.web.webSocketUrl = null;
config.redis.host = null;
config.redis.port = null;

module.exports = config