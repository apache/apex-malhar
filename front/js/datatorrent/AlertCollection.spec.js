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
var AlertCollection = require('./AlertCollection');
var Notifier = require('./Notifier');
var settings = DT.settings;

describe('AlertCollection.js', function() {
    
    var url;
    beforeEach(function() {
        this.sandbox = sinon.sandbox.create();
        this.server = sinon.fakeServer.create();

        this.sandbox.stub(Notifier, 'success');
        this.sandbox.stub(Notifier, 'error');

        this.collection = new AlertCollection({}, {
            appId: 'application_123'
        });
        
        url = settings.interpolateParams(settings.urls.Alert, { appId: 'application_123', v: 'v1' });
        
    });

    it('should load', function() {
        this.server.respondWith('GET', url,
            [200, { "Content-Type": "application/json" }, JSON.stringify({
                alerts: [
                    { name: 'alert0' },
                    { name: 'alert1' }
                ]
            })]
        );
        this.collection.fetch();
        this.server.respond();
        expect(this.collection).to.have.length(2);
        expect(this.collection.at(0).get('name')).to.equal('alert0');
        expect(this.collection.at(1).get('name')).to.equal('alert1');
    });

    it('should notify on load failure', function() {
        this.server.respondWith('GET', url,
            [404, {}, '']);
        this.collection.fetch();
        this.server.respond();
        expect(Notifier.error).to.have.been.calledOnce;
    });

    afterEach(function() {
        this.sandbox.restore();
        this.server.restore();
    });
});