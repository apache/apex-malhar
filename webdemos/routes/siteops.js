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

var redis = require('redis');
var dateFormat = require('dateformat');
var async = require('async');
var config = require('../config');

var client;
var demoEnabled = (config.siteops.redis.port && config.siteops.redis.host);

if (demoEnabled) {
    client = redis.createClient(config.siteops.redis.port, config.siteops.redis.host);
}

exports.index = function(req, res) {
    if (demoEnabled) {
        res.render('siteops');
    } else {
        res.render('error', {
            message: 'Site Operations Demo is not enabled. Please configure Redis.'
        });
    }
};

exports.clientData = function(req, res) {
    fetchValue(req, res, 9);
};

exports.totalViews = function(req, res) {
    fetchValue(req, res, 11);
};

exports.topUrlData = function(req, res) {
    fetchTop10(req, res, 2);
};

exports.topServer = function(req, res) {
    fetchTop10(req, res, 10);
};

exports.topIpData = function(req, res) {
    fetchTop10(req, res, 6);
};

exports.server404 = function(req, res) {
    fetchTop10(req, res, 8);
};

exports.topIpClientData = function(req, res) {
    fetchTop10(req, res, 3);
};

exports.url404 = function(req, res) {
    fetchTop10(req, res, 7);
};

exports.pageViewTimeData = function(req, res) {
    fetchPageViews(req.query, function(err, result) {
        res.json(result);
    });
};

exports.serverLoad = function(req, res) {
    fetchServerLoad(req.query, function(err, result) {
        res.json(result);
    });
};

function fetchValue(req, res, dbIndex) {
    var multi = client.multi();
    multi.select(dbIndex);
    multi.get(1);
    multi.exec(function (err, replies) {
        var value = parseInt(replies[1]);
        res.json(value);
    });
}

function fetchTop10(req, res, dbIndex) {
    var multi = client.multi();
    multi.select(dbIndex);
    for (var i = 0; i < 10; i++) {
        multi.get(i);
    }

    multi.exec(function (err, replies) {
        // reply 0 - select command
        // reply 1..n - get commands
        var top10 = replies.slice(1);
        res.json(top10);
    });
}

function fetchPageViews(query, resCallback) {
    var lookbackHours = query.lookbackHours;
    var url = query.url;

    var endTime = Date.now();
    var pageKeyTemplate = 'm|$date|0:mydomain.com/$page';
    var urlKeyTemplate = 'm|$date|0:$url';

    var minute = (60 * 1000);
    var result = [];

    var keyTemplate;
    var minuteKeysFn;
    if (url) {
        minuteKeysFn = function(key) { return [key] }; // 1 key
        keyTemplate = 'm|$date|0:$url'.replace('$url', url);
    } else {
        minuteKeysFn = getPageViewsMinuteKeys;
        keyTemplate = 'm|$date|0:mydomain.com/$page';
    }

    var service = new RedisService(client, 1);

    var time = endTime - lookbackHours * (60 * minute);

    // fetch all minutes serially within lookback period
    async.whilst(
        function() { return time < endTime; },
        function(callback) {
            var date = dateFormat(time, 'UTC:yyyymmddHHMM');
            var minuteKeyTemplate = keyTemplate.replace('$date', date);
            var keys = minuteKeysFn(minuteKeyTemplate);

            service.fetchMinuteTotals(keys, time, function(item) {
                result.push(item);
                callback();
            });

            time += minute;
        },
        function (err) {
            resCallback(err, result);
        }
    );
}

function fetchServerLoad(query, resCallback) {
    var lookbackHours = query.lookbackHours;
    var server = query.server;

    var endTime = Date.now();
    var minute = (60 * 1000);
    var result = [];

    var keyTemplate;
    var minuteKeysFn;
    if (server) {
        minuteKeysFn = function(key) { return [key] }; // 1 key
        keyTemplate = 'm|$date|0:$server'.replace('$server', server);
    } else {
        minuteKeysFn = getServerLoadMinuteKeys;
        keyTemplate = 'm|$date|0:server$i.mydomain.com:80';
    }

    var service = new RedisService(client, 4);

    var time = endTime - lookbackHours * (60 * minute);

    // fetch all minutes serially within lookback period
    async.whilst(
        function() { return time < endTime; },
        function(callback) {
            var date = dateFormat(time, 'UTC:yyyymmddHHMM');
            var minuteKeyTemplate = keyTemplate.replace('$date', date);
            var keys = minuteKeysFn(minuteKeyTemplate);

            service.fetchMinuteTotals(keys, time, function(item) {
                result.push(item);
                callback();
            });

            time += minute;
        },
        function (err) {
            resCallback(err, result);
        }
    );
}

function getPageViewsMinuteKeys(keyTemplate) {
    var keys = [];
    var pages = ['home.php', 'contactus.php', 'about.php', 'support.php', 'products.php', 'services.php', 'partners.php'];

    pages.forEach(function(page) {
        var key = keyTemplate.replace('$page', page);
        keys.push(key);
    });

    return keys;
}

function getServerLoadMinuteKeys(keyTemplate) {
    var keys = [];

    for (var i = 0; i < 10; i++) {
        var key = keyTemplate.replace('$i', i);
        keys.push(key);
    }

    return keys;
}

function RedisService(client, dbIndex) {
    this.client = client;
    this.dbIndex = dbIndex;
}

RedisService.prototype.fetchMinuteTotals = function(keys, time, callback) {
    var multi = this.client.multi();
    multi.select(this.dbIndex);
    keys.forEach(function(key) {
        multi.hgetall(key);
    });

    multi.exec(function (err, replies) {
        // reply 0 - select command
        // reply 1..n - hgetall commands
        var total = 0;
        for (var i = 1; i < replies.length; i++) {
            total += parseInt(replies[i][1]);
        }

        var item = {
            timestamp: time,
            view: total
        }
        callback(item);
    });
}

