var AbstractFileModel = require('./AbstractFileModel');
var BaseCollection = require('./BaseCollection');
var AbstractFileCollection = BaseCollection.extend({

    debugName: 'files',
    
    model: AbstractFileModel
    
});
exports = module.exports = AbstractFileCollection;