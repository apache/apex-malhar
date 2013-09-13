var config = require('../config');

exports.index = function(req, res) {
    if (config.web.webSocketUrl) {
        res.render('twitter', {
            webSocketUrl: config.web.webSocketUrl
        });
    } else {
        res.render('error', {
            message: 'Twitter Demo is not enabled. Please configure WebSocket URL.'
        });
    }
};