var config = {};
config.web = {};
config.gateway = {};

config.web.port = process.env.PORT || 3333;
config.gateway.host = process.env.GATEWAY_HOST || 'localhost';
config.gateway.port = process.env.GATEWAY_PORT || 3391;

module.exports = config