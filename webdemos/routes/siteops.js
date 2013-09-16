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

exports.data = function(req, res) {
    getMinutes(req.query, function(err, result) {
        res.json(result);
    });
};

function getMinutes(query, resCallback) {
    var lookbackMinutes = query.lookbackMinutes;
    var endTime = query.endTime;
    var publisher = query.publisher;
    var advertiser = query.advertiser;
    var adunit = query.adunit;

    if (!endTime) {
        endTime = Date.now();
    }

    var keyTemplate = 'm|$date';
    if (publisher) keyTemplate += '|0:' + publisher;
    if (advertiser) keyTemplate += '|1:' + advertiser;
    if (adunit) keyTemplate += '|2:' + adunit;

    var minute = (60 * 1000);
    var result = [];
    var time = endTime - lookbackMinutes * minute;

    async.whilst(
        function () { return time <= endTime; },
        function (callback) {
            var date = dateFormat(time, 'UTC:yyyymmddHHMM');
            var key = keyTemplate
                .replace('$date', date);

            client.hgetall(key, function(err, hash) {
                if (hash) {
                    var minuteItem = {
                        timestamp: time,
                        keyPattern: keyTemplate,
                        cost: parseFloat(hash[1]),
                        revenue: parseFloat(hash[2]),
                        impressions: parseFloat(hash[3]),
                        clicks: parseFloat(hash[4])
                    }
                    result.push(minuteItem);
                }
                callback();
            });

            time += minute;
        },
        function (err) {
            resCallback(err, result);
        }
    );
}

