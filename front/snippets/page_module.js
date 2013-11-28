/**
 * ${10:Page Module}
 * 
 * 
 * ${20:Description.}
 * 
*/

var _ = require('underscore');
var Notifier = require('../Notifier');
var Base = require('./base');
var ${30:${40:Page}Page} = Base.extend({
    
    pageName:'${50:page.path.from.paths}',
    
    useDashMgr: ${60:true},
    
    // Define the default dashboard configurations
    defaultDashes: [
        {
            dash_id: '${70:default}',
            widgets: [
                { widget: '${80:widget1}', id: '${90:default_widget1}' }
            ]
        }
    ],
    
    // Page set-up
    initialize: function(options) {
        // Super
        Base.prototype.initialize.call(this,options);
        
        // Get initial args
        var url_args = this.app.nav.get('url_args');
        
        $100
        
        this.defineWidgets([
            { name: '${80:widget1}', view: require('../${105:widgetdir}'), inject: {
                $107
            }}
        ]);
        
        // Load up previous or 'default' dashboard
        this.loadDashboards('${110:${70:default}}');
        
        // Listen for application id change
        this.on('change:url_args', this.onUrlChange);
    },
    
    // Listener for when arguments change
    onUrlChange: function(args) {
        $0
    }
    
    
});

exports = module.exports = ${30:${40:Page}Page};