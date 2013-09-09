var config = {};
config.redis = {};
config.web = {};

config.web.port = process.env.PORT || 3000;
config.redis.host = 'localhost';
config.redis.port = 6379;

module.exports = config