var BaseModel = require('./BaseModel');

var LicenseSectionModel = BaseModel.extend({

    defaults: {
        comment : '',
        endDate : '',
        url : '',
        processorList : {},
        constraint : '',
        startDate : ''
    }

});
exports = module.exports = LicenseSectionModel;