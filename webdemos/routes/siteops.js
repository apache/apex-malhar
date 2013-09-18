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
        var top10 = replies.slice(1);
        res.json(top10);
    });
}

