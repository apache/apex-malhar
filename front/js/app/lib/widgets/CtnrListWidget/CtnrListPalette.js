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
 * Palette view for Container List
*/
var BaseView = DT.lib.ListPalette;
var _ = require('underscore');
var kt = require('knights-templar');
var Palette = BaseView.extend({
    
    events: {
        'click .inspectItem' : 'inspectContainer',
        'click .killCtnrs': 'killContainers'
    },
    
    inspectContainer: function() {
        var ctnrId = this.getSelected()[0].get('id');
        this.nav.go('ops/apps/' + this.model.get('id') + '/containers/' + ctnrId, { trigger:true } );
    },

    killContainers: function(evt) {
        this.$('.killCtnrs').prop('disabled', true);
        var selected = this.getSelected();
        _.each(selected, function(ctnr) {
            ctnr.kill();
        });
    },

    template: kt.make(__dirname+'/CtnrListPalette.html')
    
});

exports = module.exports = Palette