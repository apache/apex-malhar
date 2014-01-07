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