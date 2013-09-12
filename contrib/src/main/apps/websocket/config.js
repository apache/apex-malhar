var path = require('path');

var config = {};
config.web = {};

config.commonResources = path.join(__dirname, '../common');
config.commonHeaderHtml = path.join(config.commonResources, '/demo-header.html');
config.web.port = process.env.PORT || 3000;
config.web.webSocketUrl = 'ws://localhost:9090/pubsub';

module.exports = config