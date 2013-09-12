var express = require('express');
var fs = require('fs');
var config = require('./config');

var app = express();

app.set('views', __dirname + '/views');
app.set('view engine', 'ejs');
app.use(express.static(__dirname + '/public'));
app.use('/common', express.static(config.commonResources));

var headerHtml = fs.readFileSync(config.commonHeaderHtml);

app.get('/twitter', function(req, res) {
    res.render('twitter', {
        webSocketUrl: config.web.webSocketUrl,
        headerHtml: headerHtml
    });
});

app.get('/mobile', function(req, res) {
    res.render('mobile', {
        webSocketUrl: config.web.webSocketUrl,
        headerHtml: headerHtml
    });
});

app.listen(config.web.port, function() {
    console.log('Node.js server started on port ' + config.web.port);
});