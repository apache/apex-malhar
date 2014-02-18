webapps
===============

Web Applications for DataTorrent Demos:
- Twitter
- Mobile
- Machine Generated Data
- Ads Dimensions
- Fraud

## Architecture

![Demos Architecture](docs/demos_architecture.png "Demos Architecture")

## Demos configuration
 Please use ```config.js``` or environment variables for configuration (DT Gateway, Redis, MongoDB, etc.).
 See ```prod_start.sh``` and ```dev_start.sh```.

## Running Demos
 Demos run on [Node.js](http://nodejs.org/).
 To run demo web application with prebuilt dependencies:

 ``` bash
    $ NODE_ENV=production node app.js
 ```

 By default application will be available at http://localhost:3003 and will connect to DT Gateway at localhost:9090.

## Running Demos in Development Mode
 Install npm dependencies:

 ``` bash
    $ npm install
 ```

 Install Bower dependencies:

 ``` bash
    $ bower install
 ```

 Start Node.js server:

 ``` bash
    $ node app.js
 ```

 Application will be available at http://localhost:3003

## Tips

 Running Node.js as a daemon with [forever](https://github.com/nodejitsu/forever)

 ``` bash
    $ npm install forever -g
    $ forever start app.js
    $ forever list
    $ forever stop <uid>
 ```

 Running Node.js on different port

 ``` bash
    $ PORT=3001 node app.js
 ```

## Links

[Express](https://github.com/visionmedia/express) Node.js web framework

[node_redis](https://github.com/mranney/node_redis) Node.js Redis client

[forever](https://github.com/nodejitsu/forever) Node.js daemon/continuous running/fault tolerance