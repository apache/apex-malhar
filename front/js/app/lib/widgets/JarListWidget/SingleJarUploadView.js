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
var Notifier = DT.lib.Notifier;
var BaseView = require('bassview');

/**
 * SingleJarUploadView
 * 
 * View that contains controls for one jar to be uploaded.
 */
var SingleJarUploadView = BaseView.extend({

	className: 'jar-to-upload',

	initialize: function(options) {
		this.uploaded = options.uploaded;
		this.listenTo(this.model, 'upload_start', function() {
			this.$('div.progress').show();
		});
		this.listenTo(this.model, 'upload_progress', function(percentage) {
			this.$('.jar_upload_progress.bar').css('width', percentage + '%');
		});
		this.listenTo(this.model, 'upload_success', function() {
			this.$('.jar_upload_progress.bar').css('width', '100%');
			this.$('div.progress').removeClass('active').removeClass('progress-striped');
		});
	},

	render: function() {

		var json, html;

		json = this.model.toJSON();

		if (this.uploaded.get(json.name)) {
			json.overwriting = true;
		} else {
			json.overwriting = false;
		}

		html = this.template(json);

		this.$el.html(html);

		this.$('.overwrite-warning').tooltip();

		return this;
	},

	events: {
		'click .remove-jar': 'removeJar',
		'click .rename-jar': 'renameJar',
		'dblclick .jar-upload-filename': 'renameJar',
		'blur input.jar-upload-filename': 'saveNewName',
		'keydown input.jar-upload-filename': 'checkForEnter'
	},

	removeJar: function() {
		this.collection.remove(this.model);
		this.remove();
	},

	renameJar: function() {
		var $input = $('<input type="text" class="jar-upload-filename" value="' + this.model.get('name') + '" />');
		this.$('.jar-upload-filename').replaceWith($input);
		$input.focus();
	},

	saveNewName: function() {
		var newName = $.trim(this.$('.jar-upload-filename').val());
		
		// check for empty
		if (newName === '') {
			Notifier.error({
				'title': DT.text('Provide a new name'),
				'text': DT.text('The name of the jar cannot be an empty string.')
			});
			return;
		}

		// add .jar extension
		if ( ! /\.jar$/.test(newName) ) {
			newName += '.jar';
		}

		if (this.model.get('name') === newName) {
			this.render();
			return;
		}

		// check for a jar to be uploaded with the same name
		if (this.collection.get(newName)) {
			Notifier.error({
				'title': DT.text('Jar name taken'),
				'text': DT.text('The name you entered is the name of another jar you are already uploading.')
			});
			return;	
		}

		this.model.set('name', newName);

		this.render();
	},

	checkForEnter: function(evt) {
		if (evt.which === 13) {
			this.saveNewName();
		} else if (evt.which === 27) {
			this.render();
		}
	},

	template: kt.make(__dirname+'/SingleJarUploadView.html','_')

});
exports = module.exports = SingleJarUploadView;