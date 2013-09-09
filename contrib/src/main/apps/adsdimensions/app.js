var async = require('async');
var express = require('express');
var redis = require("redis");
var dateFormat = require('dateformat');
var config = require('./config');

var app = express();
var client = redis.createClient(config.redis.port, config.redis.host);

app.use(express.static(__dirname + '/public'));

app.get('/data', getData);

function getData(req, res, next) {
    getMinutes(req.query, function(err, result) {
        res.json(result);
    });
}

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

app.listen(config.web.port);
console.log('Express started on port ' + config.web.port);