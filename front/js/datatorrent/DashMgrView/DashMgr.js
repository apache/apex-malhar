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
var kt = require('knights-templar');
var BaseView = require('bassview');
var WidgetList = require('./WidgetList');
var DashMgr = BaseView.extend({
    
    initialize: function(options) {
        
        var storedVisibility = localStorage.getItem('__DASHMGR__.visible');
        
        // this.collection => dashboards
        this.wClasses = options.widgets;
        this.subview("widgetlist", new WidgetList({
            collection: this.wClasses,
            dashes: this.collection
        }));
        
        if (storedVisibility !== null) {
            // gets stored as a string
            this.visible = storedVisibility === 'true' ? true : false ;
        } else {
            this.visible = false;
        }
    },
    
    render: function(){
        var markup = this.template({});
        this.$el.html(markup);
        this.$(".inner")[this.visible ? "show" : "hide"]();
        this.assign({
            ".widget-list": "widgetlist"
        });
        return this;
    },
    
    events: {
        "click .widget-manager-tab a": "toggle"
    },
    
    toggle: function(evt){
        if (evt.preventDefault) {
            evt.preventDefault();
        }
        this.visible = !this.visible;
        var newWidth, toggleMethod, curDash;
        if ( this.visible ) {
            newWidth = this.width;
            toggleMethod = "show";
        } else {
            newWidth = this.collapsedWidth;
            toggleMethod = "hide";
        }
        this.$el.width(newWidth);
        $(".dashboardMain").css("marginLeft", newWidth);
        $(".dashboardTabs").css("marginLeft", newWidth);
        this.$(".inner")[toggleMethod]();
        
        // store visible status in localStorage
        localStorage.setItem('__DASHMGR__.visible', this.visible);
        
        // trigger width change
        curDash = this.collection.find(function(dash) {
            return dash.get('selected');
        });
        
        if (!curDash) return;
        
        curDash.triggerResize();
    },
    
    visible: true,
    
    width: 220,
    
    collapsedWidth: 3,
    
    template: kt.make(__dirname+'/DashMgr.html','_')
    
});

exports = module.exports = DashMgr