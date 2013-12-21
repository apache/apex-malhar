var express = require('express');
var sockjs  = require('sockjs');
var http = require('http');
var httpProxy = require('http-proxy');
var config = require('./config');

var app = express();

var proxy = new httpProxy.HttpProxy({
  target: {
    host: config.resourceManager.host,
    port: config.resourceManager.port
  }
});

// all environments
app.use(express.favicon());
app.use(express.logger('dev'));
app.use(express.bodyParser());
app.use(express.methodOverride());
app.use(app.router);

console.log('environment: ' + app.get('env'));

if ('production' == app.get('env')) {
    app.use(express.static(__dirname + '/dist'));
} else if ('development' == app.get('env')) {
    app.use(express.static(__dirname + '/app'));
    app.use(express.errorHandler());
}

app.get('/ws/v1/cluster/apps', function(req, res) {
  proxy.proxyRequest(req, res);
});

app.get('/settings.js', function(req, res) {
  res.setHeader('Content-Type', 'application/javascript');
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', 0);

  res.send('window.settings = ' + JSON.stringify(config.settings) + ';');
});

var items = null;

var clients = {};
var clientCount = 0;
var interval;

function createBroadcast(jobId) {
  var mapValue = 0;
  var reduceValue = 0;

  function nextValue (value) {
    value += Math.random() * 5;
    value = value < 0 ? 0 : value > 100 ? 0 : value;
    return value;
  }

  function broadcast() {
      mapValue = nextValue(mapValue);
      reduceValue = (mapValue > 5) ? nextValue(reduceValue) : 0;
      reduceValue = (reduceValue > mapValue) ? mapValue : reduceValue;

      var job = {
        id: 'job_' + jobId,
        name: 'test job ' + jobId,
        state: 'RUNNING',
        mapProgress: mapValue,
        reduceProgress: reduceValue
      };
      var data = JSON.stringify({ job: job });

      var msgObject = { topic: 'contrib.summit.mrDebugger.jobResult', data: data};

      var msg = JSON.stringify(msgObject);

      for (var key in clients) {
          if(clients.hasOwnProperty(key)) {
              clients[key].write(msg);
          }
      }
  }

  return broadcast;
}

var broadcast1 = createBroadcast('111111');
var broadcast2 = createBroadcast('222222');

function broadcast() {
  broadcast1();
  broadcast2();
}

function startBroadcast () {
    interval = setInterval(broadcast, 1000);
}

var sockjsServer = sockjs.createServer();

sockjsServer.on('connection', function(conn) {
    clientCount++;
    if (clientCount === 1) {
        startBroadcast();
    }

    clients[conn.id] = conn;

    conn.on('close', function() {
        clientCount--;
        delete clients[conn.id];
        if (clientCount === 0) {
            clearInterval(interval);
        }
    });
});

var server = http.createServer(app).listen(config.port, function(){
    console.log('Express server listening on port ' + config.port);
});

sockjsServer.installHandlers(server, { prefix: '/sockjs' });