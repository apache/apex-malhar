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

var express = require("express");
var app = express();
var ejs = require('ejs');
var fs = require('graceful-fs');
var path = require('path');
var Hash = require('hashish');
var httpProxy = require('http-proxy');
var browserify = require('browserify');
var util = require('./util');
var lessm = require('less-middleware');

// Dev configuration
var config = require('./config');
// Package
var pkg = require('./package.json');

// Set up the proxy that goes to the gateway
var proxy = new httpProxy.HttpProxy({
    target: {
        host: config.gateway.host,
        port: config.gateway.port
    }
});

// Set logger
app.use(express.logger());

// Configure the app
app.configure(function(){

    // Set rendering engine to EJS
    app.engine('html', ejs.renderFile);
    app.set('view engine', 'ejs');

});

// REST API Requests
app.get('/ws/*', function(req, res) {
    proxy.proxyRequest(req, res);
});
app.post('/ws/*', function(req, res) {
    proxy.proxyRequest(req, res);
});
app.put('/ws/*', function(req, res) {
	proxy.proxyRequest(req, res);
});
app.delete('/ws/*', function(req, res) {
	proxy.proxyRequest(req, res);
});

// Main entry page
app.get('/', function(req, res) {
    res.render('index', {
        config: config,
        pkg: pkg
    });
});

// Browserify bundle
var b = browserify();
b.add('./js/start.dev.js');
app.get('/bundle.js', function(req, res) {

	util.precompileTemplates();

	res.setHeader("Content-Type", "text/javascript");

	var bundle = b.bundle({
		insertGlobals: true,
		debug: true
	});
	bundle.on('error', function(e) {
		res.end('$(function() { $("body").prepend("<p style=\'font-size:15px; padding: 10px;\'>' + e.toString().replace('"', '\"') + '</p>"); });');
	});

	var data = '';
	bundle.on('data', function(chunk) {
		data += chunk;
	});
	
	bundle.on('end', function() {
		res.end(data, 'utf8');
	});
	
});

// Compile LESS on the fly
app.use(lessm({
    dest: __dirname + '/css',
    src: __dirname + '/css',
    prefix: '/css',
    debug: true,
    sourceMap: true
}));

// Serve static files
app.use(express.static(__dirname, { maxAge: 86400000 }));

// Start the server
app.listen(config.web.port);
console.log("Server listening on port " + config.web.port);