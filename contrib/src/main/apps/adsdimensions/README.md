adsdimensions
===============

Ads Dimensions Demo with Node.js and Redis.

## Usage

 Install dependencies:

    $ npm install

 Start Node.js server:

    $ node app

 Application will be available at http://localhost:3000

## Tips

 Running Node.js as a daemon with [forever](https://github.com/nodejitsu/forever)

    $ npm install forever -g
    $ forever start app.js
    $ forever list
    $ forever stop <uid>

 Running Node.js in the background with nohup

    $ nohup node app &

 Killing an app

    $ ps -ef | grep "node app"
    $ kill -9 <PID>

 Running Node.js on different port

    $ PORT=3001 node app

## Links

[Express](https://github.com/visionmedia/express) Node.js web framework

[node_redis](https://github.com/mranney/node_redis) Node.js Redis client

[forever](https://github.com/nodejitsu/forever) Node.js daemon/continuous running/fault tolerance