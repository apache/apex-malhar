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
var Notifier = require('../Notifier');
var Poll = require('./GatewayPoll');
var M = require('./RestartModalView');

describe('RestartModalView.js', function() {
    
    var sandbox, m, dataSource, options;

    beforeEach(function() {
        sandbox = sinon.sandbox.create();

        $.fn.modal = sandbox.spy();

        _.each(['body','close','reset','restartFailed','restartSucceeded', 'render', 'doRestart'], function(method) {
            sandbox.spy(M.prototype, method);
        });

        _.each(['error','info','warning','success'], function(method){
            sandbox.stub(Notifier, method);
        });

        dataSource = {
            connect: sandbox.spy(),
            disconnect: sandbox.spy()
        };

        options = {
            dataSource: dataSource,
            message: 'Restarting the Gateway...',
            restartCompleteCallback: sandbox.spy(),
            prompt: true
        };

        m = new M(options);
    });

    afterEach(function() {
        sandbox.restore();
        m = options = null;
    });

    describe('initialize method', function() {
        
        _.each(['dataSource', 'message', 'restartCompleteCallback'], function(key) {
            it('should set ' + key + ' from passed options', function() {
                expect(m[key]).to.equal(options[key]);
            });
        });
        
        it('should set this.restarting to false', function() {
            expect(m.restarting).to.equal(false);
        });

        it('should instantiate a GatewayPoll object', function() {
            expect(m.poll).to.be.instanceof(Poll);
        });

        it('should prompt the user for restart if prompt:true is passed in the options', function() {
            expect(m.doRestart).not.to.have.been.called;
        });

        it('should NOT prompt the user if prompt:true is not passed in the options', function() {
            options.prompt = undefined;
            var m2 = new M(options);
            expect(m2.doRestart).to.have.been.calledOnce;
        });

    });

    describe('doRestart method', function() {
        
        var init_deferred, init_promise, restart_deferred, restart_promise;

        beforeEach(function() {

            init_deferred = $.Deferred();
            init_promise = init_deferred.promise();
            restart_deferred = $.Deferred();
            restart_promise = restart_deferred.promise();
            start_deferred = $.Deferred();
            start_promise = start_deferred.promise();

            m.poll = {
                initId: sandbox.stub().returns(init_promise),
                restartRequest: sandbox.stub().returns(restart_promise),
                start: sandbox.stub().returns(start_promise)
            };

            m.doRestart();
        });

        it('should call prevent default on a passed event if it is present', function() {
            var e = {
                preventDefault: sandbox.spy()
            }
            m.doRestart(e);
            expect(e.preventDefault).to.have.been.calledOnce;
        });

        it('should set restarting to true', function() {
            expect(m.restarting).to.equal(true);
        });

        it('should set the confirmText and cancelText to false', function() {
            expect(m.confirmText).to.equal(false);
            expect(m.cancelText).to.equal(false);
        });

        it('should call the render function', function() {
            expect(m.render).to.have.been.calledOnce;
        });

        it('should call initId on the GatewayPoll instance', function() {
            expect(m.poll.initId).to.have.been.calledOnce;
        });

        describe('when the initId promise is resolved', function() {

            beforeEach(function() {
                init_deferred.resolve();
            });
            
            it('should call disconnect on the dataSource', function() {
                expect(options.dataSource.disconnect).to.have.been.calledOnce;
            });

            it('should call restartRequest on the GatewayPoll instance', function() {
                expect(m.poll.restartRequest).to.have.been.calledOnce;
            });

            describe('when the restartRequest promise is resolved', function() {

                beforeEach(function() {
                    restart_deferred.resolve();
                });

                it('should call start on the GatewayPoll instance', function() {
                    expect(m.poll.start).to.have.been.calledOnce;
                });

                describe('when the start promise is resolved', function() {

                    beforeEach(function() {
                        start_deferred.resolve();
                    });

                    it('should call restartSucceeded once', function() {
                        expect(m.restartSucceeded).to.have.been.calledOnce;
                    });

                    it('should call restartSucceeded on the modal instance', function() {
                        expect(m.restartSucceeded).to.have.been.calledOn(m);
                    });
                });

                describe('when the start promise is rejected', function() {
                    
                    beforeEach(function() {
                        start_deferred.reject();
                    });                    

                    it('should call restartFailed once', function() {
                        expect(m.restartFailed).to.have.been.calledOnce;
                    });

                    it('should call restartFailed on the modal instance', function() {
                        expect(m.restartFailed).to.have.been.calledOn(m);
                    });

                });

            });

            describe('when the restartRequest promise is rejected', function() {
                beforeEach(function() {
                    restart_deferred.reject();
                });

                it('should call restartFailed once', function() {
                    expect(m.restartFailed).to.have.been.calledOnce;
                });

                it('should call restartFailed on the modal instance', function() {
                    expect(m.restartFailed).to.have.been.calledOn(m);
                });
            });

        });

        describe('when the initId promise is rejected', function() {
            
            beforeEach(function() {
                init_deferred.reject();
            });

            it('should call restartFailed on the modal instance', function() {
                expect(m.restartFailed).to.have.been.calledOn(m);
            });

        });

    });

    describe('restartSucceeded method', function() {

        beforeEach(function() {
            m.restartSucceeded();
        });

        _.each(['Notifier.success','dataSource.connect','options.restartCompleteCallback','m.close','m.reset'], function(method) {
            it('should call ' + method, function() {
                expect(eval(method)).to.have.been.calledOnce;
            });
        });

    });

    describe('restartFailed method', function() {
        beforeEach(function() {
            m.restartFailed();
        });

        _.each(['Notifier.error','m.close','m.reset'], function(method) {
            it('should call ' + method, function() {
                expect(eval(method)).to.have.been.calledOnce;
            });
        });        
    });

    describe('reset method', function() {
        beforeEach(function() {
            m.reset();
        });

        it('should set this.restarting to false', function() {
            expect(m.restarting).to.equal(false);
        });
    
        it('should set the confirm and cancel text to original', function() {
            expect(m.confirmText).to.equal(M.prototype.confirmText);
            expect(m.cancelText).to.equal(M.prototype.cancelText);
        });

        it('should call render', function() {
            expect(m.render).to.have.been.calledOnce;
        });
    });

});