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
/**
 * View for widget settings
*/

var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = require('bassview');
var Bbind = require('./Bbindings');
var SettingsView = BaseView.extend({
    
    initialize: function() {
        this.subview('widgetWidth', new Bbind.text({
            attr: 'width',
            model: this.model
        }));
        this.subview('widgetId', new Bbind.text({
            attr: 'id',
            model: this.model
        }));
        this.subview('widgetHeight', new Bbind.text({
            attr: 'height',
            model: this.model
        }));
    },
    
    render: function() {
        // Set up markup
        var json = this.model.toJSON();
        var markup = this.template(json);
        
        // Add to html
        this.$el.html(markup);
        // Assign subview
        this.assign({
            '.widgetWidth': 'widgetWidth',
            '.widgetHeight': 'widgetHeight',
            '.widgetId': 'widgetId'
        });
        this.$el.on('hidden', function(){
            this.$el.remove();
        }.bind(this));
        this.$el.modal({
            'show': true
        });
        return this;
    },
    
    template: kt.make(__dirname+'/WidgetSettingsView.html','_')
    
});
exports = module.exports = SettingsView;