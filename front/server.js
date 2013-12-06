var express = require("express");
var app = express();
var ejs = require('ejs');
var fs = require('fs');
var path = require('path');
var Hash = require('hashish');
var httpProxy = require('http-proxy');

// Dev configuration
var config = require('./config.js');

// Set up the proxy that goes to the gateway
var proxy = new httpProxy.HttpProxy({
    target: {
        host: config.gateway.host,
        port: config.gateway.port
    }
});

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

// Main entry page
app.get('/dev', function(req, res) {
    res.render('dev', config);
});

// Serve static files
app.use(express.static(__dirname, { maxAge: 86400000 }));

// Start the server
app.listen(config.web.port);
console.log("Server listening on port " + config.web.port);