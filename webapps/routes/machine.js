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

exports.index = function (req, res) {
  if (demoEnabled) {
    res.render('machine');
  } else {
    res.render('error', {
      message: 'Machine Generated Data Demo is not enabled. Please configure Redis.'
    });
  }
};

exports.data = function (req, res) {
  getMinutes(req.query, function (err, result) {
    res.json(result);
  });
};

function getMinutes(query, resCallback) {
  var lookback = query.lookback;
  var lastTimestamp = query.lastTimestamp;
  var keyParams = [query.customer, query.product, query.os, query.software1, query.software2, query.software3, query.deviceId];

  var paramKeyTemplate = '|$index:$param';
  var minuteKeyTemplate = 'm|$date';
  keyParams.forEach(function (param, index) {
    if (param) {
      var paramKey = paramKeyTemplate
        .replace('$index', index)
        .replace('$param', param);
      minuteKeyTemplate += paramKey;
    }
  });

  var minute = (60 * 1000);

  var endTime = Date.now();
  endTime -= (endTime % minute); // round to minute

  if (lastTimestamp) {
    var startTime = lastTimestamp - (lastTimestamp % minute); // round to minute
    var optimizedLookback = Math.floor((endTime - startTime)/minute) + 1;
    if (optimizedLookback <  lookback) {
      lookback = optimizedLookback;
    }
  }

  var minuteKeys = [];
  for (var i = lookback - 1; i >= 0; i--) {
    var time = endTime - (i * minute);
    var date = dateFormat(time, 'UTC:yyyymmddHHMM');
    var key = minuteKeyTemplate.replace('$date', date);
    minuteKeys.push({
      timestamp: time,
      key: key
    });
  }

  var multi = client.multi();
  minuteKeys.forEach(function(key) {
    multi.hgetall(key.key);
  });

  var minutes = [];
  var execStartTime = Date.now();
  multi.exec(function (err, replies) {
    replies.forEach(function (reply, index) {
      console.log('Redis exec time ' + (Date.now() - execStartTime) + 'ms');
      if (reply) {
        var minute = {
          timestamp: minuteKeys[index].timestamp,
          cpu: reply.cpu,
          ram: reply.ram,
          hdd: reply.hdd
        };
        minutes.push(minute);
      }
    });

    resCallback(err, minutes);
  });
}

