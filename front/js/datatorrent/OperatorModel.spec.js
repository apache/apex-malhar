var _ = require('underscore');
var WindowId = require('./WindowId');
var Model = require('./OperatorModel');

describe('OperatorModel.js', function() {
    
	var sandbox, m;

	beforeEach(function() {
	    sandbox = sinon.sandbox.create();
	    m = new Model({});
	});

	afterEach(function() {
	    sandbox.restore();
	    m = null;
	});

	_.each(['currentWindowId', 'recoveryWindowId'], function(windowIdKey) {

        it('should set the ' + windowIdKey + ' to an instance of WindowId by default', function() {
            expect(m.get(windowIdKey)).to.be.instanceof(WindowId);
        });

        it('should not use the same ' + windowIdKey + ' object as other instantiated models', function() {
            var m2 = new Model({});
            expect(m.get(windowIdKey)).not.to.equal(m2.get(windowIdKey));
        });

    });

});