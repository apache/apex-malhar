var M = require('./ClusterMetricsModel');
describe('ClusterMetricsModel.js', function() {
    
	var sandbox, m;

	beforeEach(function() {
	    sandbox = sinon.sandbox.create();
	    m = new M({});
	});

	afterEach(function() {
	    sandbox.restore();
	    m = null;
	});

	it('should add an as_of attribute whenever it has been synced', function(done) {
	    m.on('sync', function() {
	    	expect(m.get('as_of')).to.be.a('number');
	    	expect(m.get('as_of')).to.be.above(+new Date() - 20);
	    	done();
	    });

	    m.trigger('sync');
	});

});