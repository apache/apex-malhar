webapps
===============

Web Applications for DataTorrent Demos:
- Twitter
- Mobile
- Machine Generated Data
- Ads Dimensions
- Fraud

Demos run on [Node.js](http://nodejs.org/).

## Architecture

![Demos Architecture](docs/demos_architecture.png "Demos Architecture")

## Demos configuration
 - config.js
 - app/scripts/settings.js

## Running Demos
 Install dependencies:

    $ npm install

 Install Bower dependencies:

    $ bower install

 Start Node.js server:

    $ node app

 Application will be available at http://localhost:3003

## Tips

 Running Node.js as a daemon with [forever](https://github.com/nodejitsu/forever)

    $ npm install forever -g
    $ forever start app.js
    $ forever list
    $ forever stop <uid>

 Running Node.js on different port

    $ PORT=3001 node app

## Links

[Express](https://github.com/visionmedia/express) Node.js web framework

[node_redis](https://github.com/mranney/node_redis) Node.js Redis client

[forever](https://github.com/nodejitsu/forever) Node.js daemon/continuous running/fault tolerance