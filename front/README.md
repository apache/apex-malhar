DataTorrent Front
==============

Open-source, web-based user interface for use with [DataTorrent](http://datatorrent.com), a stream-processing platform for developing real-time, big data applications in Hadoop. 

Installation/Building
---------------------

After cloning the [Malhar](https://github.com/DataTorrent/Malhar) repository from Github:

    cd front
    npm install .
    make build
    
This creates a `dist` folder whose contents should be copied into the static file server root on the DataTorrent Gateway.

Tests
-------

Tests are written using the [Mocha Framework](http://visionmedia.github.io/mocha/), the [Chai Assertion Library](http://chaijs.com/), and the [Sinon Library](http://sinonjs.org/). The `suite.js` file which is located in `front/test/` includes all the tests to be run. The individual test files are located in the same directory of the file under test, and by convention end in `.spec.js`.

### CLI

To run tests via command line, run `make clitest` (or `npm test`, which proxies to the make target).

### Browser

To run the tests in the browser, run `make test`, then go to `http://localhost:3334/test/runner.html` to view the test results.


Development
-----------

To start development on the DataTorrent UI, you will need to set up a ssh tunnel to the node in a DataTorrent cluster that has the DT Gateway successfully configured and running.

### Configure

By default, this project assumes that you have a tunnel listening on port 3390 of your local machine that is connected to the Gateway node. If you are using another port, change the assignment of `config.gateway.port` in `config.js` to the appropriate port.

### Starting Bundle Server

This project uses [Browserify](http://browserify.org/) to bundle all the javascript files. This is basically like adding a "compile" step between writing code and viewing it in the browser. To streamline this process, you need to have a [beefy](https://github.com/chrisdickinson/beefy) server running on port 9222 of your local machine. This will make it so that when you make a code change, you can refresh the page and your changes will be reflected in the next load. 

Start the bundle server by running:

	make bundle


### Starting the Dev Server

Lastly, run:

	node server

Then, open `http://localhost:3333/dev` in chrome and you should see the dashboard.



The DT Global Variable
----------------------

At runtime, the variable DT is available on the global namespace and contains all the base components that the Front relies on, such as classes that model aspects of your datatorrent applications and views that are common throughout the project. To view what is on this module, take a look at the file located at `front/js/datatorent/index.js`.


Pages
-----

The file located at `front/js/app/pages.js` acts as an index for all registered pages, and contains more information therein about how to set up a page. The actual page definitions are located in `front/js/app/lib/pages/` and specify a page name, the widgets that can be instantiated, the default widget configurations (called dashboards), and the page-wide models that will be instantiated. The best way to get familiar with these is to review the code.


Widgets
-------

Widgets are simply enhanced backbone views. It is recommended (but not necessarily required) to have all widgets extend from DT.widgets.Widget, which maps to the base backbone view located at `front/js/datatorrent/WidgetView.js`.

### Location
Widgets live in `/js/App/lib/widgets`. 

### Creation

To create a boilerplate for a widget, run 

    ./bin/createWidget [-c] _WIDGET_NAME_

This will append "Widget" to the end of your widget name, so it is not necessary to include it here. The new folder that is created will be located at `front/js/app/lib/widgets/_WIDGET_NAME_Widget`.


Contributing
------------

If you would like to help improve this project, please follow the guidelines in the [section on contributing](https://github.com/DataTorrent/Malhar#contributing) in the Malhar repository. Additionally, for linting we use [JSHint](http://www.jshint.com/), so please run your code through this before submission.
