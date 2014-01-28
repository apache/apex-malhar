var BaseCollection = require('./BaseCollection');
var LicenseSectionModel = require('./LicenseSectionModel');

/**
 * License Section Collection
 */
var LicenseSectionCollection = BaseCollection.extend({

    debugName: 'License Sections',

    model: LicenseSectionModel

});
exports = module.exports = LicenseSectionCollection;