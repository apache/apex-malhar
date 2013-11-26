/**
 * <WIDGETNAME>
 * 
 * <WIDGETDESCRIPTION>
 *
*/

var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = DT.lib.WidgetView;

// class definition
var <WIDGETNAME> = BaseView.extend({
    
    initialize: function(options) {
        
        BaseView.prototype.initialize.call(this, options);
        
        // listeners, subviews, etc.
        
    },
    
    html: function() {
        
        var html = '';
        
        // generate markup here
        
        return html;
    },
    
    assignments: {
        
        // assign subviews here
        
    },
    
    template: kt.make(__dirname+'/<WIDGETNAME>.html','_')
    
});

exports = module.exports = <WIDGETNAME>;