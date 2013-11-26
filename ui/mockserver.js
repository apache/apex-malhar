var express = require('express');
var _ = require('underscore');

var app = express();


//var server = express.createServer();
//server.enable("jsonp callback");

app.use(express.bodyParser());

app.get('/alerts/v1/:appId', listAlerts);

app.get('/alerts/v1/:appId/:alertName', deleteAlert);
app.delete('/alerts/v1/:appId/:alertName', deleteAlert);

alerts = [];
for (var i = 0; i < 7; i++) {
    alerts.push({
        name: 'alert' + i,
        operatorName: 'operator' + i,
        portName: 'port' + i,
        lastTriggered: new Date().getTime() - Math.floor((Math.random() * 1000 * 60 * 60))
    });
}

function deleteAlert(req, res, next) {
    var alertName = req.params.alertName;
    console.log(new Date() + ' deleting alert ' + alertName);

    alerts = _.reject(alerts, function(a) {
        return a.name === alertName;
    });

    //res.send('');
    //res.jsonp(alerts);
    res.jsonp({});
}

function listAlerts(req, res, next) {
    var appId = req.params.appId;
    console.log(new Date() + ' getting alerts for ' + appId);
    res.jsonp( { alerts: alerts });
}

app.listen(3000);
console.log('Express started on port 3000');
