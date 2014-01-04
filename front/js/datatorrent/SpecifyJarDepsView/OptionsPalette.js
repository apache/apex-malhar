var _ = require('underscore');
var kt = require('knights-templar');
var BasePalette = require('../widgets/ListWidget/ListPalette');

var OptionsPalette = BasePalette.extend({

	initialize: function(options) {

		this.table = options.table;
	},

	events: {
		'click .toggleAll': 'toggleSelect'
	},

	toggleSelect: function() {
		
		var filtered = this.table.getFilteredRows();
		
		var allOn = _.every(filtered, function(jar) {
			return jar.selected;
		});

		var checkedNow = !allOn;

		_.each(filtered, function(row) {
			if (row.selected != checkedNow) {
				row.selected = checkedNow;
				row.trigger('change_selected', row, checkedNow);
			}
		});
	},

	template: kt.make(__dirname+'/OptionsPalette.html','_')

});

exports = module.exports = OptionsPalette;