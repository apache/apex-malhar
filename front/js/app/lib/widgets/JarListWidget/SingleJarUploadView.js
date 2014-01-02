var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = require('bassview');

/**
 * SingleJarUploadView
 * 
 * View that contains controls for one jar to be uploaded.
 */
var SingleJarUploadView = BaseView.extend({

	className: 'jar-to-upload',

	render: function() {

		var json, html;

		json = this.model.toJSON();

		if (this.collection.get(json.name)) {
			json.overwriting = true;
		} else {
			json.overwriting = false;
		}

		html = this.template(json);

		this.$el.html(html);

		this.$('.overwrite-warning').tooltip();

		return this;
	},

	template: kt.make(__dirname+'/SingleJarUploadView.html','_')

});
exports = module.exports = SingleJarUploadView;