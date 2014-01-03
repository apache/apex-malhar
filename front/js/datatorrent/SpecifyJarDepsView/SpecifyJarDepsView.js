var _ = require('underscore');
var Backbone = require('backbone');
var text = require('../text');
var kt = require('knights-templar');
var BaseView = require('../ModalView');
var DepJarFileCollection = require('../DepJarFileCollection');

/**
 * Modal view for specifying dependency jars
 */
var SpecifyJarDepsView = BaseView.extend({

	title: text('Specify Jar Dependencies'),

	confirmText: text('save dependencies'),

	initialize: function() {
		// Load in dependencies
		// this.choices = new DepJarFileCollection([]);
		// this.choices.fetch();

		// Create collection for storing deps
		// this.deps = new DepJarFileCollection([]);
	},

	body: function() {
		return this.template({
			choices: this.choices.toJSON(),
			deps: this.deps.toJSON()
		});
	},

	template: kt.make(__dirname+'/SpecifyJarDepsView.html','_')

});
exports = module.exports = SpecifyJarDepsView;