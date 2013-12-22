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
var Backbone = require('backbone');
var text = require('../text');
var kt = require('knights-templar');
var BaseView = require('../ModalView');

var AppLaunchProperty = Backbone.Model.extend({
	defaults: {
		'name':'',
		'value': ''
	}
});

var AppLaunchProperties = Backbone.Collection.extend({
	model: AppLaunchProperty,
	toObject: function() {
		var obj = {};
		this.each(function(prop) {
			obj[prop.get('name')] = prop.get('value');
		});
		return obj;
	}
});

var LaunchJarWithPropsView = BaseView.extend({

	confirmText: text('launch'),

	title: text('Launch Application with Properties'),

	initialize: function() {

		this.properties = new AppLaunchProperties([]);
		this.listenTo(this.properties, 'add remove reset', this.renderBody);

	},

	body: function() {
		return this.template({
			properties: this.properties.toJSON()
		});
	},

	events: function() {
		var events = _.extend({}, BaseView.prototype.events, {
			'click .addProperty': 'addProperty',
			'keyup .propertyField': 'updateProperties',
			'click .removeProperty': 'removeProperty'
		});
		return events;
	},

	addProperty: function() {
		this.properties.add({ id: this.properties.length + 1 });
	},

	updateProperties: function(evt) {
		var $field = $(evt.target);
		
		// get the id, value, and key of this field
		var id = $field.data('id');
		var val = $field.val();
		var key = $field.data('key');

		// update corresponding property in AppLaunchProperties
		var row = this.properties.get(id);
		if (row) {
			row.set(key, val);
		}
	},

	removeProperty: function(evt) {
		var id = $(evt.currentTarget).data('id');
		this.properties.remove(this.properties.get(id));
	},

	onConfirm: function(evt) {
		evt.preventDefault();
		var properties = this.properties.toObject();
		this.model.launch(properties);
		this.destroy();
	},

	className: 'modal launch-jar-app',

	template: kt.make(__dirname+'/LaunchJarWithPropsView.html','_')

});
exports = module.exports = LaunchJarWithPropsView;