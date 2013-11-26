var _                  = require('underscore'),
    Widget             = require('./<%= name %>'),
    WidgetDefModel     = require('../../WidgetDefModel'),
    DashModel          = require('../../DashModel'),
    WidgetClasses      = require('../../WidgetClassCollection');


describe('<%= name %>.js', function() {
   
    var sandbox, dashDef, widgetDef, options, $box;
    
    beforeEach(function() {
        // Use this for creating spies, mocks, stubs
        sandbox = sinon.sandbox.create();
        
        // Dashboard definition (mock)
        dashDef = new DashModel({ 
            widgets: new WidgetClasses([
                {
                    name: 'Widget',
                    view: Widget,
                    limit: 1
                }
            ])
        });
        
        // Widget definition (mock)
        widgetDef = new WidgetDefModel({
            'widget': 'Widget',
            'id': 'testingWidget'
        });
        
        // Options to use for constructing widget instance
        options = {
            dashboard: dashDef,
            widget: widgetDef
        };
        
        // Create an element for use in tests
        $box = $('<div id="sandbox" style="display:none;"></div>').appendTo('body');
        
    });
    
    afterEach(function() {
        sandbox.restore();
        $box.remove()
        dashDef = widgetDef = options = $box = null;
    });
    
    
    
});