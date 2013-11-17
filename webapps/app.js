/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var express = require('express');
var sockjs  = require('sockjs');
var http = require('http');
var httpProxy = require('http-proxy');
var config = require('./config');

var machine = require('./routes/machine');
var adsdimensions = require('./routes/adsdimensions');
var fraud = require('./routes/fraud');

var app = express();

var proxy = new httpProxy.RoutingProxy();

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


app.get('/machine', machine.data);
app.get('/dimensions', adsdimensions.data);
app.get('/fraud/alertCount', fraud.getAlertCount);
app.get('/fraud/randomStats', fraud.getRecentStats);
app.get('/ws/*', function(req, res) {
    proxy.proxyRequest(req, res, {
        host: config.daemon.host,
        port: config.daemon.port
    });
});

var clients = {};
var clientCount = 0;

function broadcast() {
    var random = Math.floor(Math.random() * 1000);

    var topic;
    if (random % 2 === 0) {
        topic = 'topic1';
    } else {
        topic = 'topic2';
    }
    var message = {
        topic: topic,
        data: { id: random % 10, progress: random % 100 }
    }

    for (var key in clients) {
        if(clients.hasOwnProperty(key)) {
            //var message = { random: random, clients: clientCount };
            clients[key].write(JSON.stringify(message));
        }
    }
}

function startBroadcast () {
    setInterval(broadcast, 200);
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
    });
});

var server = http.createServer(app).listen(config.web.port, function(){
    console.log('Express server listening on port ' + config.web.port);
});

sockjsServer.installHandlers(server, { prefix: '/random' });