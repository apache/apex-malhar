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

 Running Node.js in the background

    $ nohup node app &

 Killing an app

    $ ps -ef | grep "node app"
    $ kill -9 <PID>

 Running Node.js on different port

    $ PORT=3001 node app