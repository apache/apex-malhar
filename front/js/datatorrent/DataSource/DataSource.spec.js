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
var Notifier = require('../Notifier');
var DataSource = require('./DataSource');

describe('DataSource.js', function() {

	var dataSource,
		user = new Backbone.Model({
		    stramroot: 'hadoop'
		}),
		fake_host = 'test.host.com',
		sandbox;
	
	function basicSetup() {
        sandbox = sinon.sandbox.create();
        sandbox.stub(window, "WebSocket", function(){
            this.close = function() {

            }
        });
        sandbox.stub(Notifier, "warning");
        sandbox.stub(Notifier, "info");
        sandbox.stub(Notifier, "success");
        sandbox.stub(Notifier, "error");
		
		 dataSource = new DataSource(fake_host, user);
        dataSource.connect();
        sandbox.stub($, 'ajax');
	}
	
	function basicTearDown() {
        sandbox.restore();
	}
	
	describe('the constructor', function() {

		beforeEach(basicSetup);

		afterEach(basicTearDown);


	    it('should be a function', function() {
	        expect(DataSource).to.be.a('function');
	    });

	    it('should mix Backbone.Events into the object', function() {
	    	expect(dataSource.on).to.be.a('function');
	    	expect(dataSource.off).to.be.a('function');
	    	expect(dataSource.listenTo).to.be.a('function');
	    	expect(dataSource.stopListening).to.be.a('function');
	    });

	    it('should set the Daemon API version', function() {
	    	expect(dataSource.version).to.be.a('string');
	    });

	    it('should set the host to the past string', function() {
	    	expect(dataSource.host).to.equal(fake_host);
	    });

	    it('should create a WebSocket and set it to the object', function() {
	    	expect(dataSource.ws).to.be.instanceof(window.WebSocket);
	    	expect(dataSource.ws).to.be.an('object');
	    });

	    it('should create a deferred', function() {
	    	expect(dataSource.__wsCxn).to.be.an('object');
	    	expect(dataSource.__wsCxn.promise).to.be.a('function');
	    	expect(dataSource.__wsCxn.resolve).to.be.a('function');
	    });
        
        it('should throw if host is not provided', function() {
            var fn = function() {
                var badDataSource = new DataSource('', user);
                badDataSource.connect();
            }
            var fn2 = function() {
                var badDataSource = new DataSource(false, user);
                badDataSource.connect();
            }
            var fn3 = function() {
                var badDataSource = new DataSource(undefined, user);
                badDataSource.connect();
            }
            expect(fn).to.throw();
            expect(fn2).to.throw();
            expect(fn3).to.throw();
        });
        
        it('should throw if user is not provided', function() {
            var fn = function() {
                var badDataSource = new DataSource(fake_host);
                badDataSource.connect();
            }
            var fn2 = function() {
                var badDataSource = new DataSource(fake_host, false);
                badDataSource.connect();
            }
            var fn3 = function() {
                var badDataSource = new DataSource(fake_host, 'not an object');
                badDataSource.connect();
            }
            expect(fn).to.throw();
            expect(fn2).to.throw();
            expect(fn3).to.throw();
        });
        
	});

	describe('the WebSocket connection', function() {

		beforeEach(basicSetup);

		afterEach(basicTearDown);

		it('should not resolve the deferred until onopen is called', function() {
	    	expect(dataSource.__wsCxn.state()).to.equal('pending');
	    	dataSource.ws.onopen({});
	    	expect(dataSource.__wsCxn.state()).to.equal('resolved');
	    });

	    it('should resolve with the datasource as context', function(done) {
	    	dataSource.__wsCxn.done(function(ws) {
	    		expect(this).to.equal(dataSource);
	    		done();
	    	});
	    	dataSource.ws.onopen({});
	    });

	    it('should resolve with the websocket connection as argument', function(done) {
	    	dataSource.__wsCxn.done(function(ws) {
	    		expect(ws).to.equal(dataSource.ws);
	    		done();
	    	});
	    	dataSource.ws.onopen({});
	    });

	    it('should call notifier.warning when the connection closes', function() {
	    	dataSource.ws.onopen({});
	    	dataSource.ws.onclose();
	    	expect(Notifier.warning).to.have.been.calledOnce;
	    });

	    it('should call the notifier when there is an error', function() {
	    	dataSource.ws.onopen({});
	    	dataSource.ws.onerror();
	    	expect(Notifier.error).to.have.been.calledOnce;
	    });

	    it('should trigger an error if a message is malformed json', function(done) {
	    	dataSource.once('error', function(data) {
	    		done();
	    	});
	    	dataSource.ws.onmessage('{malformed: json;');
	    });
	    
	    it('should trigger an error if a message does not contain keys: type, topic, data', function() {
	        var spy = sandbox.spy();
	        sandbox.stub(dataSource.ws, 'close');
	        dataSource.ws.onmessage({ data: '{"topic":"test.topic","data":{"some":"data"}}' });
	        dataSource.ws.onmessage({ data: '{"topic":"test.topic2","type":"error"}' });
	        dataSource.ws.onmessage({ data: '{"type":"error","data":{"some":"data"}}' });
	        
	        expect(Notifier.error.callCount).to.equal(3);
	        expect(dataSource.ws.close).to.have.been.calledOnce;
	    });

	    it('should publish to specificed topic when onmessage is called', function(done) {
	        dataSource.once('example.topic', function(data) {
                expect(data).to.eql({"some": "data"});
	            done();
	        });
	        var msg = {
	            "type": "pubsub",
	            "topic": "example.topic",
	            "data": {"some": "data"}
	        }
	        
	        var msg_str = JSON.stringify(msg);
	        
	        dataSource.ws.onmessage({data: msg_str});
	    });
	    
	    it('should lazily unsubscribe to a topic if no listeners are registered', function() {
            sandbox.stub(dataSource, 'unsubscribe');
	        var msg = {
	            "type": "pubsub",
	            "topic": "example.topic",
	            "data": {"some": "data"}
	        }
	        var msg_str = JSON.stringify(msg);
	        dataSource.ws.onmessage({data: msg_str});
	        expect(dataSource.unsubscribe).to.have.been.calledOnce;
	        dataSource.unsubscribe.restore();
	    });
	    
	    it('should call Notifier.error when there is a WebSocket error', function() {
	        dataSource.ws.onerror({});
	        expect(Notifier.error).to.have.been.calledOnce;
	    });

	});
    
    describe('the API', function() {
        
        function checkForOneArg(method) {
            return function() {
                var fn = function() {
                    dataSource[method](function() {}, true);
                }
                var fn2 = function() {
                    dataSource[method]({}, true, '');
                }
                var fn3 = function() {
                    dataSource[method]('testing');
                }
                expect(fn).to.throw();
                expect(fn2).to.throw();
                expect(fn3).to.throw();
            }
        }
        
        beforeEach(basicSetup);
        
        afterEach(basicTearDown);
        
        describe('_ajax method', function() {
            
            it('_ajax method should add type to options', function() {
                dataSource._ajax('FAKE_TYPE', {
                    url: '/fake/url',
                    random: 'random'
                });
                expect($.ajax).to.have.been.calledWith({ type: 'FAKE_TYPE', url: '/fake/url', random: 'random' });
            });

            it('_ajax method should throw if no url option is supplied', function() {
                var fn = function() {
                    dataSource._ajax('GET', {
                        random: 'random'
                    });
                }
                expect(fn).to.throw();
            });
            
        });
        
        describe('verb methods', function() {
            
            it('get should call ajax with GET as type', function() {
                dataSource.get({
                    url: '/fake/url'
                });
                expect($.ajax).to.have.been.calledWith({ type: 'GET', url: '/fake/url' });
            });
            
            it('post should add type:POST to options', function() {
                dataSource.get({
                    url: '/fake/url'
                });
                expect($.ajax).to.have.been.calledWith({ type: 'GET', url: '/fake/url' });
            });

            it('post should JSON.stringify its data and change the contentType option to JSON', function() {
                var data = { key: 'value' };
                dataSource.post({
                    url: '/fake/url',
                    data: data
                });
                expect($.ajax).to.have.been.calledWith({
                    type: 'POST',
                    url: '/fake/url',
                    data: JSON.stringify(data),
                    contentType: 'application/json; charset=utf-8'
                });
            });
            
            it('put method should call ajax with PUT as type', function() {
                dataSource.put({
                    url: '/fake/url'
                });
                expect($.ajax).to.have.been.calledWith({ type: 'PUT', url: '/fake/url' });
            });

            it('delete method should call ajax with DELETE as type', function() {
                dataSource.delete({
                    url: '/fake/url'
                });
                expect($.ajax).to.have.been.calledWith({ type: 'DELETE', url: '/fake/url' });
            });
            
        });
        
        describe('_send method', function() {
            
            it('_send method should stringify the message object', function() {
                sandbox.stub(JSON, 'stringify');
                dataSource._send({ key: 'value' });
                expect(JSON.stringify).to.have.been.calledWith({ key: 'value' });
                JSON.stringify.restore();
            });

            it('_send method should use deferred.done() if ws connection has not been established', function() {
                sandbox.stub(dataSource.__wsCxn, 'done');
                dataSource._send({key:"value"});
                expect(dataSource.__wsCxn.done).to.have.been.calledOnce;
                dataSource.__wsCxn.done.restore();
            });

            it('_send method should call deferred functions when the ws connection has been established', function() {
                dataSource.ws.send = function() {}
                sandbox.stub(dataSource.ws, 'send');
                sandbox.spy(dataSource.__wsCxn, 'done');

                dataSource._send({key:"value"});
                dataSource.ws.onopen();
                expect(dataSource.ws.send).to.have.been.calledOnce;

                dataSource.ws.send.restore();
                dataSource.__wsCxn.done.restore();
            });

            it('_send method should use the ws.send method if the connection has been established', function() {
                dataSource.ws.onopen();
                dataSource.ws.send = function() {}
                sandbox.stub(dataSource.ws, 'send');
                dataSource._send({key:"value"});
                expect(dataSource.ws.send).to.have.been.calledOnce;
                expect(dataSource.ws.send).to.have.been.calledWith(JSON.stringify({key:"value"}));
                dataSource.ws.send.restore();
            });
            
        });
        
        describe('subscribe methods', function() {
            
            it('subscribe should call _send with appropriate message object', function() {
                sandbox.stub(dataSource, '_send');
                dataSource.subscribe('fake.topic');
                expect(dataSource._send).to.have.been.calledWith({type: 'subscribe', topic: 'fake.topic'});
            });

            it('unsubscribe should call _send with appropriate message object', function() {
                sandbox.stub(dataSource, '_send');
                dataSource.unsubscribe('fake.topic');
                expect(dataSource._send).to.have.been.calledWith({type: 'unsubscribe', topic: 'fake.topic'});
            });
            
        });
        
        describe('the GET methods', function() {
            
            it('getRecordingTuples method should take a single argument', checkForOneArg('getRecordingTuples'));
            
            it('getRecordingTuples method should throw if one or more are missing: startTime, operatorId, appId, limit, offset/startWindow', function() {
                var fn = function() {
                    dataSource.getRecordingTuples({
                        offset: '0',
                        limit: '20',
                        startTime: '2039902342',
                        operatorId: '1',
                    });
                }
                var fn2 = function() {
                    dataSource.getRecordingTuples({
                        offset: '0',
                        limit: '20',
                        startTime: '2039902342',
                        appId: 'application_00000000_0001'
                    });
                }
                var fn3 = function() {
                    dataSource.getRecordingTuples({
                        offset: '0',
                        limit: '20',
                        operatorId: '1',
                        appId: 'application_00000000_0001'
                    });
                }
                var fn4 = function() {
                    dataSource.getRecordingTuples({
                        limit: '20',
                        operatorId: '1',
                        appId: 'application_00000000_0001',
                        startTime: '2039902342'
                    });
                }
                var fn5 = function() {
                    dataSource.getRecordingTuples({
                        offset: '0',
                        operatorId: '1',
                        appId: 'application_00000000_0001',
                        startTime: '2039902342'
                    });
                }
                expect(fn).to.throw();
                expect(fn2).to.throw();
                expect(fn3).to.throw();
                expect(fn4).to.throw();
                expect(fn5).to.throw();
            });
            
            it('getRecordingTuples should throw if ports is provided but is not an array', function() {
                var fn = function() {
                    dataSource.getRecordingTuples({
                        offset:'0',
                        operatorId: '1',
                        limit: '10',
                        appId: 'application_00000000_0001',
                        startTime: '2039902342',
                        ports: '1,2'
                    });
                }
                expect(fn).to.throw();
            });
            
            it('getRecordingTuples method should format the data object with the data provided', function() {
                sandbox.stub(dataSource, 'get', function(options) {
                    expect(options).to.have.deep.property('data.limit', '20');
                    expect(options).to.have.deep.property('data.offset', '0');
                    expect(options).to.have.deep.property('data.ports', '1,2');
                });
                dataSource.getRecordingTuples({
                    limit: '20',
                    offset: '0',
                    ports: ['1','2'],
                    startTime: '193898234',
                    operatorId: '1',
                    appId: 'application_00000000_0001'
                });
            });
            
            it('getAlertClasses should require appId and classType as options', function() {
                var fn = function() {
                    dataSource.getAlertClasses({
                        appId: 'application_00000000_0001'
                    });
                }
                var fn2 = function() {
                    dataSource.getAlertClasses({
                        classType: 'filter'
                    });
                }
                expect(fn).to.throw();
                expect(fn2).to.throw();
            });
            
            it('getAlertClasses should take a single argument', checkForOneArg('getAlertClasses'));
            
            it('getAlertClasses should make a get request with the expected url', function() {
                sandbox.stub(dataSource, 'get', function(options) {
                    expect(options).to.have.property('url', '/alerts/v1/application_00000000_0001/fakeClasses');
                });
                dataSource.getAlertClasses({
                    appId: 'application_00000000_0001',
                    classType: 'fake'
                });
                expect(dataSource.get).to.have.been.calledOnce;
            });
            
            it('getOperatorClass should require appId and className as options', function() {
                var fn = function() {
                    dataSource.getOperatorClass({
                        appId: 'application_00000000_0001'
                    });
                }
                var fn2 = function() {
                    dataSource.getOperatorClass({
                        className: 'filter'
                    });
                }
                expect(fn).to.throw();
                expect(fn2).to.throw();
            });
            
            it('getOperatorClass should take a single argument', checkForOneArg('getOperatorClass'));
            
            it('getOperatorClass should make a get request with the expected url', function() {
                sandbox.stub(dataSource, 'get', function(options) {
                    expect(options).to.have.property('url', '/stram/v1/applications/application_00000000_0001/describeOperator');
                    expect(options).to.have.deep.property('data.class','some.fake.java.class');
                });
                dataSource.getOperatorClass({
                    appId: 'application_00000000_0001',
                    className: 'some.fake.java.class'
                });
                expect(dataSource.get).to.have.been.calledOnce;
            });
        
        });
        
        describe('the POST methods', function() {
            
            it('killApp method should take a single argument', checkForOneArg('killApp'));
            
            it('killApp method should require appId option', function() {
                var fn = function() {
                    dataSource.killApp({
                        
                    });
                }
                expect(fn).to.throw();
            });
            
            it('killApp method should make an ajax post call', function() {
                var options;
                sandbox.stub(dataSource, 'post');
                dataSource.killApp({
                    appId: 'application_00000000_0001'
                });
                options = dataSource.post.args[0][0];
                expect(dataSource.post).to.have.been.calledOnce;
            });
            
            it('startOpRecording method should take a single argument', checkForOneArg('startOpRecording'));
            
            it('startOpRecording method should require appId and operatorId options', function() {
                var fn = function() {
                    dataSource.startOpRecording({
                        operatorId: '1',
                        port: '1'
                    });
                }
                var fn2 = function() {
                    dataSource.startOpRecording({
                        appId: 'application_00000000_0001',
                        port: '1'
                    });
                }
                expect(fn).to.throw();
                expect(fn2).to.throw();
            });
            
            it('startOpRecording method should call the post method with appropriate data', function() {
                var options;
                sandbox.stub(dataSource, 'post');
                dataSource.startOpRecording({
                    appId: 'application_00000000_0001',
                    operatorId: '1'
                });
                options = dataSource.post.args[0][0];
                expect(dataSource.post).to.have.been.calledOnce;
                dataSource.post.restore();
            });
            
            it('stopOpRecording method should take a single argument', checkForOneArg('stopOpRecording'));
            
            it('stopOpRecording method should call _actOnOpRecording', function() {
                sandbox.stub(dataSource, '_actOnOpRecording');
                dataSource.stopOpRecording({
                    appId: 'application_00000000_0001',
                    operatorId: '1'
                });
                expect(dataSource._actOnOpRecording).to.have.been.calledOnce;
                dataSource._actOnOpRecording.restore();
            });
            
        });
        
    });
});