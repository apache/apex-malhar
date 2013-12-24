var express = require("express");
var app = express();
var ejs = require('ejs');
var fs = require('graceful-fs');
var path = require('path');
var Hash = require('hashish');
var httpProxy = require('http-proxy');
var browserify = require('browserify');

// Dev configuration
var config = require('./config.js');

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

// Main entry page
app.get('/', function(req, res) {
    res.render('index', config);
});

// Browserify bundle
var b = browserify();
b.add('./js/start.dev.js');
app.get('/bundle.js', function(req, res) {

	res.setHeader("Content-Type", "text/javascript");

	var bundle = b.bundle({
		insertGlobals: true,
		debug: true
	});
	bundle.on('error', function(e) {
		res.end('$(function() { $("body").prepend("<p style=\'font-size:15px; padding: 10px;\'>' + e.toString() + '</p>"); });');
	});

	var data = '';
	bundle.on('data', function(chunk) {
		data += chunk;
	});
	
	bundle.on('end', function() {
		res.end(data, 'utf8');
	});
	
});

// Serve static files
app.use(express.static(__dirname, { maxAge: 86400000 }));

// Start the server
app.listen(config.web.port);
console.log("Server listening on port " + config.web.port);