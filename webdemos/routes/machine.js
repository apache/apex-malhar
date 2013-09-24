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
    console.log('data');
    getMinutes(req.query, function(err, result) {
        console.log('__result');
        console.log(result);
        res.json(result);
    });
};

function getMinutes(query, resCallback) {
    var lookbackHours = query.lookbackHours;
    var customer = query.customer;
    var product = query.product;
    var os = query.os;
    var software1 = query.software1;
    var software2 = query.software2;
    var software3 = query.software3;

    var keyTemplate = 'm|$date';

    var minute = (60 * 1000);
    var result = [];
    var endTime = Date.now();
    var time = endTime - lookbackHours * (60 * minute);

    // fetch all minutes serially within lookback period
    async.whilst(
        function() { return time < endTime; },
        function(callback) {
            var date = dateFormat(time, 'UTC:yyyymmddHHMM');
            var key = keyTemplate.replace('$date', date);


            client.hgetall(key, function(err, hash) {
                console.log(key);
                console.log(hash);
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

