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
var Collection = require('./ModeCollection');

describe('ModeCollection.js', function() {
    
    var sandbox, c;

    beforeEach(function() {
        sandbox = sinon.sandbox.create();
        c = new Collection([
            { id: 'test', href: '#test', name: 'Test' },
            { id: 'test2', href: '#test2', name: 'Test2' },
            { id: 'test3', href: '#test3', name: 'Test3' }
        ]);
    });

    afterEach(function() {
        sandbox.restore();
    });

    describe('clearActive method', function() {
        
        var m;

        beforeEach(function() {
            m = c.get('test');
            m.set('active', true);            
        });

        it('should make all models have active:false', function() {
            c.clearActive();
            expect(m.get('active')).to.equal(false);
        });

        it('should accept options for setting active', function() {
            var spy = sandbox.spy();
            c.on('change:active', spy);
            c.clearActive({ silent: true });
            expect(spy).not.to.have.been.called;
        });

    });

});