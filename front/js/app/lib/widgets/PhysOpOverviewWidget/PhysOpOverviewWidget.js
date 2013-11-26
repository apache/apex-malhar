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
 * Overview widget for physical operator
 * 
*/
var _ = require('underscore');
var formatters = DT.formatters;
var templates = DT.templates;
var kt = require('knights-templar');
var BaseView = DT.widgets.OverviewWidget;
var PhysOpOverviewWidget = BaseView.extend({

    html: function() {
        var json = this.model.serialize();
        json['cpuPercentageMA'] = formatters.percentageFormatter(json['cpuPercentageMA'], true);
        json['containerLink'] = templates.container_link({
            appId: json.appId,
            containerId: json.container,
            containerIdShort: json.container.replace(/.*_(\d+)$/, '$1')
        });
        return this.template(json);
    },
    
    template: kt.make(__dirname+'/PhysOpOverviewWidget.html','_')
    
});
exports = module.exports = PhysOpOverviewWidget;