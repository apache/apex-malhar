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

// To run these tests Redis server should be running and have data
// (there should be active SiteOps application that writes to Redis).

var siteops = require('../routes/siteops');

exports.testTop10 = function(test) {
    test.expect(1);
    siteops.topUrlData(null, {
        json: function(data) {
            //console.log(data);
            test.equal(data.length, 10);
            test.done();
        }
    });
}

exports.testPageViewTimeData = function(test) {
    test.expect(1);
    var req = {
        query: {
            lookbackHours: 1,
        }
    }
    siteops.pageViewTimeData(req, {
        json: function(data) {
            test.equal(data.length, req.query.lookbackHours * 60); // 60 minutes in 1 hour
            test.done();
        }
    });
};

exports.testPageViewTimeDataURL = function(test) {
    test.expect(1);
    var req = {
        query: {
            lookbackHours: 2,
            url: 'mydomain.com/home.php'
        }
    }
    siteops.pageViewTimeData(req, {
        json: function(data) {
            test.equal(data.length, req.query.lookbackHours * 60); // 60 minutes in 1 hour
            test.done();
        }
    });
};