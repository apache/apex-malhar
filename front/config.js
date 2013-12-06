var config = {};
config.web = {};
config.daemon = {};

config.web.port = process.env.PORT || 3333;
config.daemon.host = 'localhost';
config.daemon.port = 3390;

module.exports = config