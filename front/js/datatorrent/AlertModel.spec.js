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
var AlertModel = require('./AlertModel');
var AlertCollection = require('./AlertCollection');
var Notifier = require('./Notifier');
var settings = DT.settings;

describe('AlertModel.js', function() {
    var sandbox, server, url;
    beforeEach(function() {
        sandbox = sinon.sandbox.create();
        server = sinon.fakeServer.create();

        sandbox.stub(Notifier, 'success');
        sandbox.stub(Notifier, 'error');

        this.model = new AlertModel({
            name: 'testalert',
            appId: 'application_00000000_0001'
        }, {});
        url = settings.interpolateParams(settings.urls.Alert, { appId: 'application_00000000_0001', v: 'v1' }) + '/testalert';
        this.collection = new AlertCollection([], {
            appId: 'testapp'
        });
        this.collection.add(this.model);
    });

    it('should create', function() {
        sandbox.stub(Backbone.Model.prototype, 'save');
        this.model.create();
        expect(Backbone.Model.prototype.save).to.have.been.calledOnce;
    });

    it('should notify on delete success', function() {
        server.respondWith("DELETE", url, [200, { "Content-Type": "application/json" }, '{}']);
        this.model.delete();
        server.respond();
        expect(Notifier.success).to.have.been.calledOnce;
    });

    it('should notify on delete failure', function() {
        server.respondWith("DELETE", url , [404, {}, ''] );
        this.model.delete();
        server.respond();
        expect(Notifier.error).to.have.been.calledOnce;
    });

    afterEach(function() {
        sandbox.restore();
        server.restore();
    });
});