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
		this.model = new LicenseModel({});
		this.collection = this.model.get('licenseAgents');
		this.model.fetch();
		this.collection.fetch();
		this.listenTo(this.model, 'sync', this.renderBody);
		this.listenTo(this.collection, 'sync', this.renderBody);
	},

	body: function() {
		var json = this.model.toJSON();
		var html = this.template(json);
		return html;
	},

	confirmText: text('close'),

	cancelText: false,

	template: kt.make(__dirname+'/LicenseModalView.html','_')

});
exports = module.exports = LicenseModalView;