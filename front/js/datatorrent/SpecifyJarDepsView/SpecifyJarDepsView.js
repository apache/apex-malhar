var _ = require('underscore');
var formatters = require('../formatters');
var Backbone = require('backbone');
var text = require('../text');
var kt = require('knights-templar');
var Tabled = require('../tabled');
var BaseView = require('../ModalView');
var DepJarFileCollection = require('../DepJarFileCollection');
var option_columns = [
    { id: "selector", key: "selected", label: "", select: true, width: 40, lock_width: true },
    { id: "name", key: "name", label: "file Name", filter: "like", sort_value: "a", sort: "string" },
    { id: "modificationTime", label: "mod date", key: "modificationTime", sort: "number", filter: "date", format: "timeStamp" },
    { id: "size", label: "size", key: "size", sort: "number", filter: "number", format: formatters.byteFormatter, width: 80 },
    { id: "owner", label: "owner", key: "owner", sort: "string", filter: "like", width: 60 }
];
var choice_columns = [
	{ id: "handle", key: "name", label: "order", width: 50, lock_width: true },
	{ id: "name", key: "name", label: "File Name" }
];

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

		// Create collection for storing choices
		this.choices = new DepJarFileCollection([]);

		// Create options table
		this.subview('options', new Tabled({
			collection: this.options,
			columns: option_columns,
			row_sorts: ['name'],
			table_width: 515,
			adjustable_width: false
		}));

		// Create choices table
		this.subview('choices', new Tabled({
			collection: this.choices,
			columns: choice_columns,
			table_width: 515,
			adjustable_width: false
		}));
	},

	body: function() {
		return this.template({ });
	},

	postRender: function() {
		this.subview('options').resizeTableToCtnr();
		this.subview('choices').resizeTableToCtnr();
	},

	assignments: {
		'.dep-options .table-target': 'options',
		'.dep-choices .table-target': 'choices'
	},

	template: kt.make(__dirname+'/SpecifyJarDepsView.html','_')

});
exports = module.exports = SpecifyJarDepsView;