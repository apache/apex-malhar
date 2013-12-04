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
var Page = require('./AppInstancePageView');
var _ = require('underscore'), Backbone = require('backbone');
var AppModel = DT.lib.ApplicationModel;
var LogicalOperatorCollection = DT.lib.LogicalOperatorCollection;
var Notifier = DT.lib.Notifier;

describe('AppInstancePageView.js', function() {
    
	var sandbox, p, app, appFetch, dataSource;

	beforeEach(function() {

		// create the sandbox
		sandbox = sinon.sandbox.create();
		
		// create the mock app (dashboard app, not DT app)
	    app = new Backbone.View({});
	    appFetch = function(options) {
	    	options.success({
	    		state: 'RUNNING',
	    		finalStatus: 'UNDEFINED'
	    	});
	    }

	    // create mock datasource
	    app.dataSource = dataSource = _.extend({
	    	subscribe: sandbox.spy()
	    }, Backbone.Events);

	    // spy on functions
	    sandbox.spy(Page.prototype, 'onAppFetch');
	    sandbox.spy(Page.prototype, 'setLocalKey');
	    sandbox.spy(Page.prototype, 'defineWidgets');
	    sandbox.spy(Page.prototype, 'loadDashboards');

	    // stub notifier
	    sandbox.stub(Notifier, 'warning');

	    // stub the appmodel methods
	    sandbox.spy(AppModel.prototype, 'setOperators');
	    sandbox.spy(AppModel.prototype, 'setContainers');
	    sandbox.spy(AppModel.prototype, 'fetch');
	    sandbox.stub($, 'ajax', function(options) {
	    	appFetch(options);
	    });
	    sandbox.stub(AppModel.prototype, 'subscribe');

	    // create the page itself
	    p = new Page({
	    	app: app,
	    	pageParams: {
	    		appId: 'application_0001'
	    	}
	    });


	});

	afterEach(function() {
	    sandbox.restore();
	    p = app = null;
	});

	it('should create an application model as its model', function() {
		expect(p.model).to.be.instanceof(AppModel);    
	});

	it('should set the application id as the one passed in pageParams', function() {
	    expect(p.model.get('id')).to.equal('application_0001');
	});

	it('should pass the datasource to the application model', function() {
	    expect(p.model.dataSource).to.equal(app.dataSource);
	});

	it('should call setOperators and setContainers', function() {
	    expect(p.model.setOperators).to.have.been.calledOnce;
	    expect(p.model.setContainers).to.have.been.calledOnce;
	});

	it('should call a synchronous fetch on the application', function() {
	    var args = p.model.fetch.getCall(0).args;
	    expect(args[0]).to.have.property('async', false);
	});

	it('should call subscribe on the application model', function() {
	    expect(p.model.subscribe).to.have.been.calledOnce;
	});

	it('should call the onAppFetch method', function() {
	    expect(p.onAppFetch).to.have.been.calledOnce;
	    expect(p.onAppFetch).to.have.been.calledWith({ appId: 'application_0001' });
	});

	it('should set a listener on the state of the application', function() {
		p.model.set('state', 'FINISHED');
		expect(Notifier.warning).to.have.been.calledOnce;
	});

	describe('the onAppFetch method', function() {

		it('should call the defineWidgets method', function() {
			expect(p.defineWidgets).to.have.been.calledOnce;    
		});

	    it('should call the setLocalKey function with the :RUNNING extension if the app is running', function() {
	    	_.extend(Page.prototype.defaults)
			expect(p.setLocalKey).to.have.been.calledWith(':RUNNING');
	    });

	    it('should call the setLocalKey function with the :RUNNING extension if the app is accepted', function() {
	    	appFetch = function(options) {
				options.success({
		    		state: 'ACCEPTED',
		    		finalStatus: 'UNDEFINED'
		    	});
			}
			p2 = new Page({
		    	app: app,
		    	pageParams: {
		    		appId: 'application_0001'
		    	}
		    });
			expect(p2.setLocalKey).to.have.been.calledWith(':RUNNING');
	    });

	    it('should call loadDashboards', function() {
	        expect(p.loadDashboards).to.have.been.calledOnce;
	    });

	});

	describe('the getLogicalOperators method', function() {
	    
		var p2;

		beforeEach(function() {

			p2 = new Page({
		    	app: app,
		    	pageParams: {
		    		appId: 'application_0001'
		    	}
		    });

		});

		it('should return the logicalOperators object if it is already on the page object', function() {
		    var ops = {
		    	fetch: function() {},
		    	subscribe: function() {}
		    };
		    p2.logicalOperators = ops;
		    expect(p2.getLogicalOperators()).to.equal(ops);
		});

		it('should create a new collection of logical operators if one does not already exist', function() {
		    var ops = p2.getLogicalOperators();
		    expect(ops).to.be.instanceof(LogicalOperatorCollection);
		});

		it('should fetch and subscribe to the logical operators collection if the app state is running', function() {
		    var ops = {
		    	fetch: sandbox.spy(),
		    	subscribe: sandbox.spy()
		    };
		    p2.logicalOperators = ops;
		    p2.getLogicalOperators();
		    expect(ops.fetch).to.have.been.calledOnce;
		    expect(ops.subscribe).to.have.been.calledOnce;
		});

		it('should set a listener for state change on the application if the finalStatus is undefined and the state is not running', function() {
		    appFetch = function(options) {
		    	options.success({
		    		'state': 'ACCEPTED',
		    		'finalStatus': 'UNDEFINED'
		    	});
		    }
		    var p3 = new Page({
		    	app: app,
		    	pageParams: {
		    		appId: 'application_0001'
		    	}
		    });
		    var ops = {
		    	fetch: sandbox.spy(),
		    	subscribe: sandbox.spy()
		    };
		    p3.logicalOperators = ops;

		    p3.getLogicalOperators();

			expect(ops.fetch).not.to.have.been.called;
		    expect(ops.subscribe).not.to.have.been.called;

		    p3.model.set('state', 'RUNNING');
		    expect(ops.fetch).to.have.been.calledOnce;
		    expect(ops.subscribe).to.have.been.calledOnce;
		});

	});

});