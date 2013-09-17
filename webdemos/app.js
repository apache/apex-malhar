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
var config = require('./config');

var main = require('./routes/index');
var twitter = require('./routes/twitter');
var mobile = require('./routes/mobile');
var ads = require('./routes/ads');
var siteops = require('./routes/siteops');

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
app.get('/siteops', siteops.index);
app.get('/siteops/data', siteops.data);

app.listen(config.web.port, function() {
    console.log('Node.js server started on port ' + config.web.port);
});