var config = {};
config.web = {};
config.gateway = {};

config.web.port = process.env.PORT || 3333;
config.gateway.host = 'node0-cdh.aws.datatorrent.com';
config.gateway.port = 9090;

module.exports = config