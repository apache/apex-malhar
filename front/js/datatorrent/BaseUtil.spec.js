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
var Notifier = require('./Notifier');

var settings = {
    version: 'v1',
    urls: {
        'example' : '/example/url/path/with/:var',
        'example2': '/example/url/with/version/:v/and/:param',
        'example3': '/example/url/:v'
    },
    topics: {
        'example' : '/example/topic/path/with/:var',
        'example2': '/example/topic/with/version/:v/and/:param',
        'example3': '/example/topic/:v'
    },
    actions: {
        'example' : '/example/action/path/with/:var',
        'example2': '/example/action/with/version/:v/and/:param',
        'example3': '/example/action/:v'
    },
    interpolateParams: function(string, params) {
        return string.replace(/:(\w+)/g, function(match, paramName) {
            return params[paramName];
        });
    }
}

describe('BaseUtil.js', function() {

    var sandbox;

    beforeEach(function() {
        sandbox = sinon.sandbox.create();
        sandbox.stub(Notifier, 'error');
        sandbox.stub(window, 'LOG');

    });

    afterEach(function() {
        sandbox.restore();
    });
    
    describe('the API', function() {
        _.each(['resourceURL','resourceTopic','resourceAction','subscribeToTopic','fetchError','quietFetchError','responseFormatError'], function(method){
            it('should expose the ' + method + ' method', function() {
                expect(BaseUtil[method]).to.be.a('function');
            });
        }, this);
    });

    _.each({'resourceURL':'url','resourceTopic':'topic','resourceAction':'action'}, function(entity, method) {
        describe('the ' + method + ' method', function() {
                
            var context = {
                settings: settings
            };

            it('should construct a ' + entity + ' with given settings.' + entity + 's.path and parameters', function() {
                var result = BaseUtil[method].call(context, 'example', { 'var': 'variables' });
                expect(result).to.equal('/example/' + entity + '/path/with/variables');
            });
            
            it('should autofill version params in ' + entity + 's', function() {
                var result = BaseUtil[method].call(context, 'example2', { 'param': 'parameter' });
                expect(result).to.equal('/example/' + entity + '/with/version/' + settings.version + '/and/parameter');
            });
            
            it('should not require a param object', function(){
                var result = BaseUtil[method].call(context, 'example3');
                expect(result).to.equal('/example/' + entity + '/' + settings.version);
            });
            
            it('should throw if the ' + entity + ' does not exist', function() {
                var fn = function() {
                    BaseUtil[method].call(context, 'notRegistered');
                }
                expect(fn).to.throw;
            });

        });
    }, this);

    describe('the fetchError method', function() {
        it('should call Notifier.error with an object containing title and text attrs', function() {
            BaseUtil.fetchError({},{ status: '404', statusText: 'Not Found'},{});
            expect(Notifier.error).to.have.been.calledOnce;
        });
    });

    describe('the quietFetchError method', function() {
        it('should call the window.LOG function', function() {
            BaseUtil.quietFetchError({},{ status: '404', statusText: 'Not Found'},{});
            expect(window.LOG).to.have.been.calledOnce;
        });
    });

    describe('the responseFormatError method', function() {
        it('should call Notifier.error with an object containing title and text attrs', function() {
            BaseUtil.responseFormatError({},{ status: '404', statusText: 'Not Found'},{});
            expect(Notifier.error).to.have.been.calledOnce;
        });
    });
    
});