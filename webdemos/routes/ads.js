var redis = require('redis');
var dateFormat = require('dateformat');
var async = require('async');
var config = require('../config');

var client;
var demoEnabled = (config.redis.port && config.redis.host);

if (demoEnabled) {
    client = redis.createClient(config.redis.port, config.redis.host);
}

exports.index = function(req, res) {
    if (demoEnabled) {
        res.render('ads');
    } else {
        res.render('error', {
            message: 'Ads Dimensions Demo is not enabled. Please configure Redis.'
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

