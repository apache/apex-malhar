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
var BaseView = DT.lib.ListPalette;

var LogicalOpListPalette = BaseView.extend({

    events: {
        'click .inspectItem': 'inspectItem'
    },

    inspectItem: function(e) {
        var selected = this.getSelected()[0];
        this.nav.go('ops/apps/' + this.appId + '/logicalOperators/' + selected.get('logicalName'));
    }


});

exports = module.exports = LogicalOpListPalette;