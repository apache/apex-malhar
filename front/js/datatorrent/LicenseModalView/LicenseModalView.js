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
var text = require('../text');
var LicenseModel = require('../LicenseModel');
var BaseView = require('../ModalView');

/**
 * Modal that shows license information
 */
var LicenseModalView = BaseView.extend({

	title: text('License Information'),

	initialize: function() {
		var agent = this.model.get('agent');
		this.listenTo(agent, 'sync', this.renderBody);
	},

	body: function() {
		var json = this.model.toJSON();
		var html = this.template(json);
		return html;
	},

	events: {
		'click .cancelBtn': 'onCancel',
		'click .confirmBtn': 'onConfirm'
	},

	confirmText: text('close'),

	cancelText: false,

	template: kt.make(__dirname+'/LicenseModalView.html','_')

});
exports = module.exports = LicenseModalView;