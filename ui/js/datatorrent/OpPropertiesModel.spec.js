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
var Backbone = require('backbone');
var OpPropertiesModel = require('./OpPropertiesModel');
var Notifier = require('./Notifier');
var settings = DT.settings;

describe('OpPropertiesModel.js', function() {
    var url;
    beforeEach(function() {
        this.sandbox = sinon.sandbox.create();
        this.server = sinon.fakeServer.create();

        this.sandbox.stub(Notifier, 'success');
        this.sandbox.stub(Notifier, 'error');

        this.model = new OpPropertiesModel({}, {
            appId: 'application_123',
            operatorName: 'GenerateX'
        });
        url = settings.interpolateParams(settings.urls.OpProperties, { v: 'v1', appId: 'application_123', operatorName: 'GenerateX' });
    });

    it('should update model on load success', function() {
        this.server.respondWith('GET', url,
            [200, { "Content-Type": "application/json" }, JSON.stringify({
                key1: 'value1',
                key2: 'value2'
            })]);
        this.model.fetch();
        this.server.respond();
        expect(this.model.get('key1')).to.equal('value1');
        expect(this.model.get('key2')).to.equal('value2');
    });

    it('should notify on load failure', function() {
        this.server.respondWith('GET', url,
            [404, {}, '']);
        this.model.fetch();
        this.server.respond();
        expect(Notifier.error).to.have.been.calledOnce;
    });

    afterEach(function() {
        this.sandbox.restore();
        this.server.restore();
    });
});