var BaseView = DT.lib.ListPalette;

var LogicalOpListPalette = BaseView.extend({

    events: {
        'click .inspectItem': 'inspectItem'
    },

    inspectItem: function(e) {
        var selected = this.getSelected()[0];
        this.nav.go('ops/apps/' + this.appId + '/logicalOperators/' + selected.get('logicalName'));
    }


});

exports = module.exports = LogicalOpListPalette;