var config = {};
config.resourceManager = {};

config.port = process.env.PORT || 3000;
config.resourceManager.host = 'localhost';
config.resourceManager.port = '8088';

// client settings (passed to the browser)
config.settings = {};
config.settings.hadoop = {};
config.settings.topic = {};

// WebSocket URL is ws://<gateway hostname>:<gateway port>/pubsub
config.settings.webSocketURL = 'ws://localhost:9090/pubsub';

config.settings.hadoop.version = '2';
config.settings.hadoop.api = 'v1';
config.settings.hadoop.host = config.resourceManager.host;
config.settings.hadoop.resourceManagerPort = config.resourceManager.port;
config.settings.hadoop.historyServerPort = '19888';

config.settings.topic.job = 'contrib.summit.mrDebugger.jobResult';
config.settings.topic.map = 'contrib.summit.mrDebugger.mapResult';
config.settings.topic.reduce = 'contrib.summit.mrDebugger.reduceResult';
config.settings.topic.counters = 'contrib.summit.mrDebugger.counterResult';

module.exports = config
