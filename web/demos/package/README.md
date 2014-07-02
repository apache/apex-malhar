Web Demos
===============

Web Application for DataTorrent Demos:
- Twitter URLs
- Twitter Hashtags
- Mobile
- Machine Generated Data
- Ads Dimensions
- Fraud

## Running Demos
 Demos run on [Node.js](http://nodejs.org/).
 To run demo web server use ```start.sh``` or launch application with Node.js:

 ``` bash
    $ node app.js
 ```

 By default application will be available at http://localhost:3003 and will connect to DT Gateway at localhost:9090.

## Demos configuration
 Please use ```config.js``` or environment variables (```start.sh```) for configuration (DT Gateway, Redis, MongoDB, etc.).

 Applications are automatically discovered by name (to show stats like "Events/sec"). See the following settings in ```config.js```:
 - settings.twitterUrls.appName
 - settings.twitterHashtags.appName
 - settings.mobile.appName
 - settings.machine.appName
 - settings.dimensions.appName
 - settings.fraud.appName

## Architecture and Development

See web application source code  [web demos](https://github.com/DataTorrent/Malhar/tree/master/web/demos)

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