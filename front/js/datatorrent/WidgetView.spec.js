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
var WidgetView = require('./WidgetView');
var _ = require('underscore');
var Backbone = require('backbone');

describe('Widget View', function() {
    
	var spy_sizeTo100, spy_grabWidthResizer, spy_grabHeightResizer, mockWidgetDef, mockDashDef, $elem, elem, view, sandbox;
    
    // fake jquery plugins
    $.fn.tooltip = function() {};
    $.fn.sortable = function() {};
    $.fn.draggable = function() {};

    beforeEach(function() {

        sandbox = sinon.sandbox.create();
    	spy_sizeTo100 = sandbox.spy(WidgetView.prototype, 'sizeTo100');
    	spy_grabWidthResizer = sandbox.spy(WidgetView.prototype, '_grabWidthResizer');
        spy_grabHeightResizer = sandbox.spy(WidgetView.prototype, '_grabHeightResizer');

    	mockWidgetDef = new Backbone.Model({
	    	width: 75,
            height: 'auto',
	    	id: 'mockWidgetDef',
            widget: 'SomeFakeWidget'
	    });

	    mockDashDef = new Backbone.Model({
	    	dash_id: 'mockDashDef'
	    });

	    $elem = $('<div id="sandbox"></div>').appendTo('body');

	    elem = $elem[0];

	    view = new WidgetView({
	        el: elem,
	        widget: mockWidgetDef,
	        dashboard: mockDashDef
	    });

        view.render();
    });

    afterEach(function() {
        sandbox.restore();
        $elem.remove();
        view.remove();
    });
    
    it('should store the widget and dash definition objects as attributes', function() {
    	expect(view.widgetDef).to.equal(mockWidgetDef);
    	expect(view.dashDef).to.equal(mockDashDef);
    });

    it('should set the initial width (%) of the widget definition, minus 0.3 for padding', function() {
    	expect(elem.style.width).to.equal('74.7%');
    });
    
    it('should change width when the widget model changes width', function() {
        mockWidgetDef.set('width', 50);
        expect(elem.style.width).to.equal('49.7%');
    });
    
    it('should not subtract the 0.3% padding when the width is 100%', function() {
        mockWidgetDef.set('width', 100);
        expect(elem.style.width).to.equal('100%');
    });

    describe('the compId method', function() {

    	it('should return a composite string', function() {
    		expect(view.compId()).to.equal('mockDashDef.mockWidgetDef');
	    });

	    it('should be able to attach additional arguments', function() {
	    	expect(view.compId('1','2','3')).to.equal('mockDashDef.mockWidgetDef.1.2.3');
	    });

    });

    describe('the sizeTo100 method', function() {

    	it('should set the width attribute of the mockWidgetDef to 100', function() {
    		view.sizeTo100();
    		expect(mockWidgetDef.get('width')).to.equal(100);
    	});

    	it('should be called when the resizer is double-clicked', function() {
    		// sinon.spy(view, 'sizeTo100');
    		var expectedCallCount = spy_sizeTo100.callCount + 1;
    		var dblclick = $.Event('dblclick');
    		view.$('.widget-width-resize').trigger(dblclick);
    		expect(spy_sizeTo100.callCount).to.equal(expectedCallCount);
    	});

    });

    describe('the grabWidthResizer method', function() {

    	var expectedCallCount, total, ten_percent, mousedown, mousemove, originalWidth, expectedWidth;

    	beforeEach(function() {

    		expectedCallCount = spy_grabWidthResizer.callCount + 1;

    		// reduce width by 10%
    		total = view.$el.parent().width();
    		ten_percent = total * 0.1;
    		mousedown = $.Event('mousedown', {
    			clientX: 1000,
    			originalEvent: {
    				preventDefault: function() {}
    			}
    		});
    		mousemove = $.Event('mousemove', {
    			clientX: 1000 - ten_percent
    		});
    		originalWidth = mockWidgetDef.get('width');
    		expectedWidth = originalWidth - 10;

    		view.$('.widget-width-resize')
    			.trigger(mousedown)
    			.trigger(mousemove)
    			.trigger('mouseup');
    	})

    	it('should be triggered when there is a mousedown event on the resizer', function() {
    		expect(spy_grabWidthResizer.callCount).to.equal(expectedCallCount);
    	});

    	// it('should change the width of the widgetDef according to change in clientX', function() {
    	// 	expect(mockWidgetDef.get('width')).to.equal(expectedWidth);
    	// });

    	afterEach(function() {
    		expectedCallCount = total = ten_percent = mousedown = mousemove = originalWidth = expectedWidth = undefined;
    	});

    });

    describe('the grabHeightResizer method', function() {

        var expectedCallCount, total, ten_percent, mousedown, mousemove, originalHeight, expectedHeight;

        beforeEach(function() {

            expectedCallCount = spy_grabWidthResizer.callCount + 1;

            // reduce height by 10%
            total = view.$el.height();
            mousedown = $.Event('mousedown', {
                clientY: 1000,
                originalEvent: {
                    preventDefault: function() {}
                }
            });
            mousemove = $.Event('mousemove', {
                clientY: 990
            });
            originalHeight = view.$el.height();
            expectedHeight = originalHeight - 10;

            view.$('.widget-height-resize')
                .trigger(mousedown)
                .trigger(mousemove)
                .trigger('mouseup');
        });

        afterEach(function() {
            expectedCallCount = total = ten_percent = mousedown = mousemove = originalHeight = expectedHeight = undefined;
        });

        it('should be triggered when there is a mousedown event on the resizer', function() {
            expect(spy_grabHeightResizer.callCount).to.equal(expectedCallCount);
        });

        // it('should change the height of the widgetDef according to change in clientY', function() {
        //     expect(mockWidgetDef.get('height')).to.equal(expectedHeight);
        // });
        
    });
        
});