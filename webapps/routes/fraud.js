var Db = require('mongodb').Db;
var Server = require('mongodb').Server;
var Deferred = require('jquery-deferred').Deferred;


// Create deferred for mongo connection
var mongoCxn = Deferred();

// Connect to the mongo database
var db = new Db('frauddetect', new Server(host, port));
db.open(function(err, mongoclient){
    console.log('Database connection opened');
    mongoCxn.resolve();
});

// Function for sending requests to mongo,
// ensures the connectino to the database has been made
function mongoRequest(fn) {
    var state = mongoCxn.state();
    if (state == 'resolved') {
        fn();
    } else if (state == 'pending') {
        mongoCxn.done(fn)
    }
}

// Retrieves all types of alerts
function getAlerts(req, res) {
    
    // binAlerts
    // db.collection('binAlerts')
    //     .find( {merchantId: "Wal-Mart", terminalId: 5, zipCode: 94087, time : { $gt: 1381989199000, $lte: 1381989399000} } );
    // 
    // 
    // 
    // var collection = req.params['collectionName'];
    // var since;
    // 
    // if (req.query.since) {
    //     since = req.query.since;
    // } else if (req.query.last) {
    //     since = +new Date() - req.query.last * 1000;
    // } else {
    //     since = +new Date() - 1000;
    // }
    // 
    // mongoRequest(function() {
    //     var binAlerts = db.collection(collection);
    //     txStats.find({ 'time': {$gt: since}}).toArray(function(err, list) {
    //         if (err) {
    //             console.log('error occurred retrieving one transaction stats item');
    //             return;
    //         }
    //         res.send(list)
    //     });
    // });
    
}

exports.getAlerts = getAlerts;