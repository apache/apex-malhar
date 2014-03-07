var BaseView = require('bassview');
var StepView = BaseView.extend({

    initialize: function(options) {
        options = options || {};
        this.template = options.template;
        this.issues = options.issues;
        this.properties = options.properties;
    },

    render: function() {
        var html = this.template({
            //issues: this.issues.toJSON(),
            //properties: this.properties.toJSON(),
            //model: this.model.toJSON()
        });
        this.$el.html(html);
        if (this.assignments) {
            this.assign(this.assignments);
        }
        return this;
    }

});
exports = module.exports = StepView;