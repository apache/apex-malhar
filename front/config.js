var config = {};
config.web = {};
config.gateway = {};

config.web.port = process.env.PORT || 3333;
config.gateway.host = 'localhost';
config.gateway.port = 3390;

module.exports = config