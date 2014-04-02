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
var ModalView = require('./ModalView');

describe('ModalView.js', function() {
    
    var sandbox;

    beforeEach(function() {
        sandbox = sinon.sandbox.create();
        $.fn.modal = sandbox.spy();
    });

    afterEach(function() {
        sandbox.restore();
    });

    it('should pick up launchOptions from options on initialize', function() {
        var launchOptions = { backdrop: 'static' };
        var v = new ModalView({ launchOptions: launchOptions });
        expect(v.launchOptions).to.equal(launchOptions);
    });

    describe('the launch method', function() {

        it('should call $().modal', function() {
            var launchOptions = { backdrop: 'static' };
            var v = new ModalView({ launchOptions: launchOptions });
            v.launch();
            expect($.fn.modal).to.have.been.calledOnce;
        });

        it('should take options that override view.launchOptions', function() {
            var launchOptions = { backdrop: 'static', show: true };
            var v = new ModalView({ launchOptions: launchOptions });
            v.launch({ backdrop: true });
            expect($.fn.modal.getCall(0).args[0]).to.eql({ backdrop: true, show: true });
        });
        
    });

});