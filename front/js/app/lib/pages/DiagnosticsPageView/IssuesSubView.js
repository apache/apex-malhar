var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = require('bassview');
var IssuesSubView = BaseView.extend({

    initialize: function(options) {
        this.collection = new DT.lib.ConfigIssueCollection([]);
        this.fetchPromise = this.collection.fetch();
        this.fetchPromise.always(this.render.bind(this));
    },

    render: function() {
        var json = {
            fetchState: this.fetchPromise.state(),
            issues: this.collection.toJSON()
        }
        var html = this.template(json);
        this.$el.html(html);
        return this;
    },

    template: kt.make(__dirname+'/IssuesSubView.html')

});
exports = module.exports = IssuesSubView;