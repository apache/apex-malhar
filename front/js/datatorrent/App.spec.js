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
var assert = require('assert');
var mock = require('mock');
// var RealApp = require('./app');
var App = mock('./app', {
    DataSource: function() {
        this.getUIVersion = function(cb) {
            cb('0.0.0');
        }
    }
}, require);

describe('an app class', function() {
    
    it('should export a function', function() {
        assert.equal(typeof App, "function");
    });
    
    it('should throw when the ws connection cant be established', function() {
        assert.throws(function() {
            var app = new App({
                host: 'test.host'
            });
        }, 'Did not throw when ws was not established');
    });
    
    it('should have all the necessary parts', function() {
        var app = new App({
            host: 'test.host'
        });
        assert(typeof app.nav !== 'undefined');
        assert(typeof app.loader !== 'undefined');
        assert(typeof app.dataSource !== 'undefined');
        assert(typeof app.user !== 'undefined');
    });
    
});