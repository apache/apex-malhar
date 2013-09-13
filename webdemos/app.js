var express = require('express');
var config = require('./config');

var main = require('./routes/index');
var twitter = require('./routes/twitter');
var mobile = require('./routes/mobile');
var ads = require('./routes/ads');

var app = express();

app.set('views', __dirname + '/views');
app.set('view engine', 'ejs');
app.use(app.router);
app.use(express.static(__dirname + '/public'));

app.get('/', main.index);
app.get('/twitter', twitter.index);
app.get('/mobile', mobile.index);
app.get('/ads', ads.index);
app.get('/ads/data', ads.data);

app.listen(config.web.port, function() {
    console.log('Node.js server started on port ' + config.web.port);
});