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
var BaseUtil = require('./BaseUtil');
var settings = {
    version: 'v1',
    urls: {
        'example' : '/example/path/with/:var',
        'example2': '/example/with/version/:v/and/:param',
        'example3': '/example/:v'
    },
    topics: {
        'example' : 'example.path.with.:var',
        'example2': 'example'
    },
    interpolateParams: function(string, params) {
        return string.replace(/:(\w+)/g, function(match, paramName) {
            return params[paramName];
        });
    }
}

describe('BaseUtil.js', function() {
    
    describe('the resourceURL method', function() {
            
        var context = {
            settings: settings
        };

        it('should construct a url with given settings.urls.path and parameters', function() {
            var result = BaseUtil.resourceURL.call(context, 'example', { 'var': 'variables' });
            expect(result).to.equal('/example/path/with/variables');
        });
        
        it('should autofill version params in urls', function() {
            var result = BaseUtil.resourceURL.call(context, 'example2', { 'param': 'parameter' });
            expect(result).to.equal('/example/with/version/' + settings.version + '/and/parameter');
        });
        
        it('should not require a param object', function(){
            var result = BaseUtil.resourceURL.call(context, 'example3');
            expect(result).to.equal('/example/' + settings.version);
        });
        
        it('should throw if the url does not exist', function() {
            var fn = function() {
                BaseUtil.resourceURL.call(context, 'notRegistered');
            }
            expect(fn).to.throw;
        });

    });
    
    describe('the resourceTopic method', function() {
            
        var context = {
            settings: settings
        };

        it('should construct a topic with given settings.topics.path and parameters', function() {
            var result = BaseUtil.resourceTopic.call(context, 'example', { 'var': 'variables' });
            expect(result).to.equal('example.path.with.variables');
        });
        
        it('should not require a param object', function(){
            var result = BaseUtil.resourceTopic.call(context, 'example2');
            expect(result).to.equal('example');
        });
        
        it('should throw if the url does not exist', function() {
            var fn = function() {
                BaseUtil.resourceTopic.call(context, 'notRegistered');
            }
            expect(fn).to.throw;
        });

    });
    
});