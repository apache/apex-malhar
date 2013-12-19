var _ = require('underscore'), Backbone = require('backbone');
var W = require('./OverviewWidget');

describe('OverviewWidget.js', function() {
    
	var sandbox, Child;

	beforeEach(function() {
	    sandbox = sinon.sandbox.create();
	    Child = W.extend({
	    	overview_items: [
	    		{
	    			key: 'key1',
	    			label: 'Key 1',
	    			title: 'title for key1'
	    		},
	    		{
	    			key: 'key2',
	    			label: 'Key 2',
	    			value: function(key2) {
	    				return 'key2: ' + key2
	    			}
	    		},
	    		{
	    			key: 'key3',
	    			label: 'Key 3',
	    			title: function(key3) {
	    				return 'title for key3: ' + key3
	    			}
	    		}
	    	]
	    });

	});

	afterEach(function() {
	    sandbox.restore();
	});

	describe('html method', function() {
	    
		var c, html, $html;

		beforeEach(function() {
		    c = new Child({
		    	model: new Backbone.Model({
		    		'key1': 'value1',
		    		'key2': 'value2',
		    		'key3': 'value3'
		    	}),
		    	dashboard: new Backbone.Model({}),
		    	widget: new Backbone.Model({})
		    });
		    html = c.html();
		    $html = $('<div>').append(html);
		});

		it('should return a string of (overview_items.length) div.overview-item elements', function() {
		    
		    expect($html.find('.overview-item').length).to.equal(3);

		});

		it('should add a title to the div.overview-item if the "title" is present in overview_items definition', function() {
		    
			expect($html.find('.overview-item:eq(0)').attr('title')).to.equal('title for key1');

		});

		it('should add calculate a title if the "title" is a function in overview_items definition', function() {
		    
			expect($html.find('.overview-item:eq(2)').attr('title')).to.equal('title for key3: value3');

		});

		it('should add the value for the appropriate key to the span.value of each overview-item', function() {
		    expect($html.find('.overview-item:eq(0) span.value').text()).to.equal('value1');
		    expect($html.find('.overview-item:eq(1) span.value').text()).to.equal('key2: value2');
		    expect($html.find('.overview-item:eq(2) span.value').text()).to.equal('value3');
		});

		it('should add the label for the appropriate key to the span.key of each overview-item', function() {
		    expect($html.find('.overview-item:eq(0) span.key').text()).to.equal('Key 1');
		    expect($html.find('.overview-item:eq(1) span.key').text()).to.equal('Key 2');
		    expect($html.find('.overview-item:eq(2) span.key').text()).to.equal('Key 3');
		});

		it('should use the model\'s serialize method if it is present', function() {
		    c.model.serialize = function() {
		    	return {
		    		'key1': 'value1',
		    		'key2': 'value2',
		    		'key3': 'value3'
		    	};
		    }
		    sandbox.spy(c.model, 'serialize');
		    c.html();
		    expect(c.model.serialize).to.have.been.calledOnce;
		});

	});

});