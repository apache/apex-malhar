var path = require('path');

var config = {};
config.redis = {};
config.web = {};

config.commonResources = path.join(__dirname, '../common');
config.commonHeaderHtml = path.join(config.commonResources, '/demo-header.html');
config.web.port = process.env.PORT || 3000;
config.redis.host = 'localhost';
config.redis.port = 6379;

module.exports = config