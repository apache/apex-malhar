var express = require("express");
var app = express();
var server = require("http").createServer(app);
var WebSocketServer = require('ws').Server;
var fs = require('fs');
var path = require('path');
var stream = require('stream');
var Hash = require('hashish');
var mgr = require('./util');
var Stream = require('stream');

// Pipe random app list out
var sendApps = function(req, res){
    var num = Math.round(Math.random() * 10);
    var file = path.join(__dirname, 'mock_data', 'apps'+num+'.txt');
    var fstream = fs.createReadStream(file);
    fstream.pipe(res);
}
// Pipe random op list out
var sendOps = function(req, res){
    var num = Math.round(Math.random() * 10);
    var file = path.join(__dirname, 'mock_data', 'ops'+num+'.txt');
    var fstream = fs.createReadStream(file);
    fstream.pipe(res);
}

// Mocking the Daemon HTTP Requests
app.get('/resourcemanager/v1/cluster/apps', sendApps );
app.get('/stram/v1/applications/:appId/operators', sendOps);

// Serving static files
app.use(express.static(__dirname, { maxAge: 86400000 }));

// Spin up server
server.listen(3333); console.log("Server listening on port 3333");

// Set up ws server
wsServer = new WebSocketServer({server: server});
wsServer.on('connection', function(ws){
    var socketid = mgr.connect(ws);
    ws.on('message', function(raw) {
        
        var message = JSON.parse(raw);
        switch(message.type) {
            case "subscribe":
                mgr.subscribe(socketid, message.topic);
            break;
            case "unsubscribe":
                mgr.unsubscribe(socketid, message.topic);
            break;
        }
        
    });

    ws.on('close', function(reasonCode, description) {
        mgr.disconnect(socketid);
    });
});

function pollApps(){
    var clients = mgr.clients;
    Hash(clients)
    .filter(function(val, key){
        return val.subscriptions.hasOwnProperty('apps.list');
    })
    .forEach(function(val, key){
        var socket = val.socket;
        var num = Math.round(Math.random() * 10);
        var file = path.join(__dirname, 'mock_data', 'apps'+num+'.txt');
        var contents = fs.readFileSync(file);
        socket.send(JSON.stringify({"type":"data","topic":"apps.list","data":JSON.parse(contents)}));
    })
}
function pollOps(){
    var clients = mgr.clients;
    var topic = ''; // topic to publish on
    Hash(clients)
    .filter(function(val, key){
        for(var k in val.subscriptions) {
            if ( /apps\.application_\d+_\d+\.operators\.list/.test(k) ) {
                topic = k;
                return true;
            }
        }
        return false;
    })
    .forEach(function(val, key){
        var socket = val.socket;
        var num = Math.round(Math.random() * 10);
        var file = path.join(__dirname, 'mock_data', 'ops'+num+'.txt');
        var contents = fs.readFileSync(file);
        socket.send(JSON.stringify({"type":"data","topic":topic,"data":JSON.parse(contents)}));
    })
}


// Begin publishing
setInterval(function(){
    pollApps();
    pollOps();
}, 1000)


