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
//app.use(express.logger());
app.use(app.router);
app.use(express.static(__dirname + '/public'));

app.get('/', main.index);
app.get('/twitter', twitter.index);
app.get('/mobile', mobile.index);
app.get('/ads', ads.index);
app.get('/ads/data', ads.data);

// Site Operations Demo
app.get('/siteops', redirectToMain);
app.get('/siteops/main', siteops.index);
// Totals
app.get('/siteops/clientData', siteops.clientData);
app.get('/siteops/totalViews', siteops.totalViews);
// Top 10
app.get('/siteops/topUrlData', siteops.topUrlData);
app.get('/siteops/topServer', siteops.topServer);
app.get('/siteops/topIpData', siteops.topIpData);
app.get('/siteops/topIpClientData', siteops.topIpClientData);
app.get('/siteops/url404', siteops.url404);
app.get('/siteops/server404', siteops.server404);
// Charts
app.get('/siteops/pageViewTimeData', siteops.pageViewTimeData);
app.get('/siteops/serverLoad', siteops.serverLoad);


function redirectToMain(req, res) {
    var url = req.url;
    if (url.slice(-1) !== '/') {
        url += '/';
    }
    res.redirect(url + 'main');
}

app.listen(config.web.port, function() {
    console.log('Node.js server started on port ' + config.web.port);
});