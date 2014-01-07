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

var ChoicesList = BaseView.extend({

	initialize: function(options) {
		this.parent = options.parent;
		this.listenTo(this.collection, 'add remove', this.render);
	},

	render: function() {

		if (this.hasBeenSortabled) {
			this.$el.sortable('destroy');
			this.hasBeenSortabled = false;
		}
		this.collection.sort();
		var json = {
			jars: this.collection.toJSON()
		};

		var html = this.template(json);
		this.$el.html(html);

		// Allow rows to be sortable
		this.$el.sortable({
            axis: 'y',
            helper: 'clone',
            placeholder: 'item-placeholder',
            stop: _.bind(function(evt, ui) {
                this.parent.choice_order = this.$el.sortable('toArray', { attribute: 'data-name' });
                
            }, this)
        });

        this.hasBeenSortabled = true;

        return this;
	},

	template: kt.make(__dirname+'/ChoicesList.html','_')

});

exports = module.exports = ChoicesList;