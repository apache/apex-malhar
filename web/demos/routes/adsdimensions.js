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
var _ = require('underscore');

var client;
var demoEnabled = (config.adsdimensions.redis.port && config.adsdimensions.redis.host);

if (demoEnabled) {
  client = redis.createClient(config.adsdimensions.redis.port, config.adsdimensions.redis.host);
  if (config.adsdimensions.redis.dbIndex) {
    client.select(config.adsdimensions.redis.dbIndex);
  }
}

exports.data = function (req, res) {
  getMinutes(req.query, function (err, result) {
    var forcedDelay = config.machine.forcedDelay; //TODO
    if (!forcedDelay) {
      res.json(result);
    } else {
      setTimeout(function () {  // for testing purpose only
        res.json(result);
      }, forcedDelay);
    }
  });
};

function getMinutes(query, resCallback) {
  var lookback = query.lookback;
  var includeLastMinute = ('true' === query.includeLastMinute);
  var lastTimestamp = query.lastTimestamp;
  var keyParams = [query.publisher, query.advertiser, query.adunit];

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

  var execStartTime = Date.now();
  multi.exec(function (err, replies) {
    var execTime = (Date.now() - execStartTime);

    var lastIndex = -1;
    var minutes = [];
    replies.forEach(function (reply, index) {
      if (reply) {
        lastIndex = index;
        var minute = {
          timestamp: minuteKeys[index].timestamp,
          cost: parseFloat(reply[1]),
          revenue: parseFloat(reply[2]),
          impressions: parseFloat(reply[3]),
          clicks: parseFloat(reply[4])
        };
        minutes.push(minute);
      }
    });

    var lastKey = (lastIndex >= 0) ? minuteKeys[lastIndex].key : 'none';
    var lastKeyQueried = _.last(minuteKeys).key;

    if (minutes.length && !includeLastMinute) {
      minutes.pop();
    }

    var result = {
      minutes: minutes,
      keysQueried: minuteKeys.length,
      lastKeyQueried: lastKeyQueried,
      lastKey: lastKey,
      queryTime: execTime
    };

    console.log('Redis exec time ' + execTime +
      'ms. Replies: ' + replies.length +
      '. Last key with data: ' + lastKey);

    resCallback(err, result);
  });
}

