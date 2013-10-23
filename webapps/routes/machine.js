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
var demoEnabled = (config.machine.redis.port && config.machine.redis.host);

if (demoEnabled) {
    client = redis.createClient(config.machine.redis.port, config.machine.redis.host);
    client.select(config.machine.redis.dbIndex);
}

exports.index = function(req, res) {
    if (demoEnabled) {
        res.render('machine');
    } else {
        res.render('error', {
            message: 'Machine Generated Data Demo is not enabled. Please configure Redis.'
        });
    }
};

exports.data = function(req, res) {
    getMinutes(req.query, function(err, result) {
        res.json(result);
    });
};

function getMinutes(query, resCallback) {
    var lookback = query.lookback;
    var keyParams = [query.customer, query.product, query.os, query.software1, query.software2, query.software3, query.deviceId];

    var paramKeyTemplate = '|$index:$param';
    var minuteKeyTemplate = 'm|$date';
    keyParams.forEach(function(param, index) {
        if (param) {
            var paramKey = paramKeyTemplate
                .replace('$index', index)
                .replace('$param', param);
            minuteKeyTemplate += paramKey;
        }
    });

    var minute = (60 * 1000);
    var result = [];
    var endTime = Date.now();
    endTime -= (endTime % minute); // round to minute
    var time = endTime - (lookback * minute);

    // fetch all minutes serially within lookback period
    async.whilst(
        function() { return time < endTime; },
        function(callback) {
            var date = dateFormat(time, 'UTC:yyyymmddHHMM');
            var key = minuteKeyTemplate.replace('$date', date);

            client.hgetall(key, function(err, hash) {
                if (hash) {
                    var minuteItem = {
                        timestamp: time,
                        cpu: hash['cpu'],
                        ram: hash['ram'],
                        hdd: hash['hdd']
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

