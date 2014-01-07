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
var BasePalette = require('../widgets/ListWidget/ListPalette');

var OptionsPalette = BasePalette.extend({

	initialize: function(options) {

		this.table = options.table;
	},

	events: {
		'click .toggleAll': 'toggleSelect'
	},

	toggleSelect: function() {
		
		var filtered = this.table.getFilteredRows();
		
		var allOn = _.every(filtered, function(jar) {
			return jar.selected;
		});

		var checkedNow = !allOn;

		_.each(filtered, function(row) {
			if (row.selected != checkedNow) {
				row.selected = checkedNow;
				row.trigger('change_selected', row, checkedNow);
			}
		});
	},

	template: kt.make(__dirname+'/OptionsPalette.html','_')

});

exports = module.exports = OptionsPalette;