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
var text = require('./text');
var kt = require('knights-templar');
var BaseView = require('bassview');
var ModalView = BaseView.extend({

	initialize: function(options) {
		options = options || {};
		if (options.hasOwnProperty('launchOptions')) {
			this.launchOptions = options.launchOptions;
		}
	},

	title: 'No title',

	confirmText: text('save'),

	cancelText: text('cancel'),

	className: 'modal',

    closeBtn: true,

	render: function() {

		var html = this.BASE_MODAL_TEMPLATE({
			title: this.title,
			body: this.body(),
			confirmText: this.confirmText,
			cancelText: this.cancelText,
            closeBtn: this.closeBtn
		});

		this.$el.html(html);

		if (typeof this.assignments === 'object') {
			this.assign(this.assignments);
		}

		if (typeof this.postRender === 'function') {
			this.postRender();
		}

		return this;
	},

	renderBody: function() {

		var html = this.body();
		this.$('.modal-body').html(html);
		return this;

	},

	addToDOM: function() {
		$('body').append(this.render().$el);
		return this;
	},

	events: {
		'click .cancelBtn': 'onCancel',
		'click .confirmBtn': 'onConfirm'
	},

	launchOptions: {
		show: true
	},

	launch: function(options) {
		options = options || {};
		options = _.defaults(options, this.launchOptions);
		this.$el.modal(options);
		this.delegateEvents();
	},

	close: function(e) {
		if (e && typeof e.preventDefault === 'function') {
            e.preventDefault();
        }
		this.$el.modal('hide');
	},

	destroy: function() {
		this.close();
		this.remove();
	},

	// Override this to pass html into body of modal
	body: function() {
		return 'I ain\'t got no body';
	},

	onConfirm: function(evt) {
		evt.preventDefault();
		this.destroy();
	},

	onCancel: function(evt) {
		evt.preventDefault();
		this.destroy();
	},

	BASE_MODAL_TEMPLATE: kt.make(__dirname+'/ModalView.html','_')

});
exports = module.exports = ModalView;