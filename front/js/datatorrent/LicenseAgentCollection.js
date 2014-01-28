var BaseCollection = require('./BaseCollection');
var LicenseAgentModel = require('./LicenseAgentModel');

/**
 * Collection representing one or more license agents
 */
var LicenseAgentCollection = BaseCollection.extend({

    debugName: 'License Agents',

    model: LicenseAgentModel,

    url: function() {
        return this.resourceURL('LicenseAgent');
    },

    responseTransform: 'licenseAgents'

});
exports = module.exports = LicenseAgentCollection;