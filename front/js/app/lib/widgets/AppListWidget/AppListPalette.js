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
var ListPalette = DT.lib.ListPalette;
var Palette = ListPalette.extend({

    events: {
        'click .inspectApp': 'goToApp',
        'click .killApp': 'killApp',
        'click .shutdownApp': 'shutdownApp',
        'click .killApps': 'killApps',
        'click .shutdownApps': 'shutdownApps'
    },
    
    killApp: function() {
        this.getSelected()[0].kill(this.dataSource);
    },
    
    shutdownApp: function() {
        this.getSelected()[0].shutdown(this.dataSource);
    },
    
    killApps: function() {
        var c = confirm(DT.text('Are you sure you want to kill the selected apps?'));
        if (c) {
            _.each(this.getSelected(), function(app) {
                if (app.get('state') === "RUNNING") {
                    app.kill(this.dataSource, true);
                }
            }, this);
        }
        this.deselectAll();
    },
    
    shutdownApps: function() {
        var c = confirm(DT.text('Are you sure you want to shutdown the selected apps?'));
        if (c) {
            _.each(this.getSelected(), function(app) {
                if (app.get('state') === "RUNNING") {
                    app.shutdown(this.dataSource, true);
                }
            }, this);
        }
        this.deselectAll();
    },
    
    goToApp: function() {
        var id = this.getSelected()[0].get('id');
        this.nav.go('ops/apps/'+id, { trigger: true});
    },
    
    template: kt.make(__dirname+'/AppListPalette.html', '_'),
    
});

exports = module.exports = Palette