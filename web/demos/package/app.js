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

app.use(express.static(__dirname + config.web.staticDir));

if ('development' == app.get('env')) {
    app.use(express.errorHandler());
}

app.get('/machine', machine.data);
app.get('/dimensions', adsdimensions.data);
app.get('/fraud/alertCount', fraud.getAlertCount);
app.get('/fraud/randomStats', fraud.getRecentStats);
app.get('/ws/*', function(req, res) {
    proxy.proxyRequest(req, res, {
        host: config.gateway.host,
        port: config.gateway.port
    });
});

app.get('/settings.js', function(req, res) {
  res.setHeader('Content-Type', 'application/javascript');
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', 0);

  res.send('window.settings = ' + JSON.stringify(config.settings) + ';');
});

http.createServer(app).listen(config.web.port, function(){
    console.log('Express server listening on port ' + config.web.port);
});