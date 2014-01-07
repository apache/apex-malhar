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
var formatters = require('../formatters');
var Backbone = require('backbone');
var text = require('../text');
var kt = require('knights-templar');
var Tabled = require('../tabled');
var BaseView = require('../ModalView');
var DepJarFileCollection = require('../DepJarFileCollection');
var OptionsPalette = require('./OptionsPalette');
var option_columns = require('./option_columns');
var ChoicesList = require('./ChoicesList');

/**
 * Modal view for specifying dependency jars
 */
var SpecifyJarDepsView = BaseView.extend({

	title: text('Specify Jar Dependencies'),

	confirmText: text('save dependencies'),

	initialize: function() {

		// Load in dependencies
		this.options = new DepJarFileCollection([]);
		this.options.fetch();

		// Create options table
		this.subview('options', new Tabled({
			collection: this.options,
			columns: option_columns,
			row_sorts: ['name'],
			table_width: 515,
			adjustable_width: false
		}));

		// Create palettes
		this.subview('options_palette', new OptionsPalette({
			collection: this.options,
			table: this.subview('options')
		}));

		// Create choices table
		this.subview('choices', new ChoicesList({
			collection: this.collection,
			parent: this
		}));

		// Set up comparator for choices
		var choice_order = this.choice_order = [];
		var self = this;
		this.collection.comparator = function(row1, row2) {
			return self.choice_order.indexOf(row1.get('name')) - choice_order.indexOf(row2.get('name'));
		}

		// Listen for selection changes
		this.listenTo(this.options, 'change_selected', function(row, selected) {

			var name = row.get('name');

			if (selected) {
				if (!this.collection.get(row)) {
					choice_order.push(name);
					this.collection.add(row);
				}
			} else {
				if (this.collection.get(row)) {
					choice_order.splice(choice_order.indexOf( name, 1 ));
					this.collection.remove(row);
				}
			}
		});

		// this.subview('choices_palette', new ChoicesPalette({
		// 	collection: this.collection
		// }));
	},

	body: function() {
		return this.template({ });
	},

	postRender: function() {
		this.subview('options').resizeTableToCtnr();
	},

	assignments: {
		'.dep-options .table-target': 'options',
		'.dep-choices .table-target': 'choices',
		'.dep-options .palette-target': 'options_palette',
		// '.dep-choices .palette-target': 'choices_palette'
	},

	onConfirm: function(evt) {
		evt.preventDefault();
		this.model.submitDependencies();		
		this.destroy();
	},

	template: kt.make(__dirname+'/SpecifyJarDepsView.html','_')

});
exports = module.exports = SpecifyJarDepsView;