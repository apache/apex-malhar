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
var _ = require('underscore');
var BaseUtil = require('./BaseUtil');
var settings = require('./settings');
var BaseCollection = require('./BaseCollection');

describe('BaseCollection.js', function() {
    
    _.each(['resourceURL', 'resourceTopic'], function(method) {
        it('should have a ' + method + ' method', function() {
            expect(BaseCollection.prototype[method]).to.be.a('function');
            expect(BaseCollection.prototype[method]).to.equal(BaseUtil[method]);
        });
    });
   
    it('should have the settings object attached to it', function() {
        expect(BaseCollection.prototype.settings).to.equal(settings);
    });

    describe('the initialize method', function() {
        it('should look for "silentErrors" in options and set quietFetchError to fetchError', function() {
            var c = new BaseCollection([], { silentErrors: true });
            expect(c.fetchError).to.equal(BaseUtil.quietFetchError);
        });
    });
    
});