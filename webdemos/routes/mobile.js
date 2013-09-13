var config = require('../config');

exports.index = function(req, res) {
    if (config.web.webSocketUrl) {
        res.render('mobile', {
            webSocketUrl: config.web.webSocketUrl
        });
    } else {
        res.render('error', {
            message: 'Mobile Demo is not enabled. Please configure WebSocket URL.'
        });
    }
};