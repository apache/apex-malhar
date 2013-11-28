var hat = require('hat');
var rack = hat.rack();
var clients = {};

// Returns generated id
var connect = function(socket){
    var id = rack();
    clients[id] = {
        id: id, 
        socket: socket,
        subscriptions: {}
    }
    return id;
}
var disconnect = function(id){
    delete clients[id].subscriptions;
    delete clients[id];
}
var subscribe = function(id, topic){
    clients[id].subscriptions[topic] = true;
}
var unsubscribe = function(id, topic){
    delete clients[id].subscriptions[topic];
}

var self = {
    connect: connect,
    disconnect: disconnect,
    subscribe: subscribe,
    unsubscribe: unsubscribe,
    clients: clients
}
exports = module.exports = self