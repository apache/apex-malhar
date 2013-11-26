DataTorrent UI
==============

Open-source, web-based user interface for use with [DataTorrent](http://datatorrent.com), a stream-processing platform for developing real-time, big data applications in Hadoop. 

Installation/Building
---------------------

After cloning the [Malhar](https://github.com/DataTorrent/Malhar) repository from Github:

	cd ui
    npm install .
    make build
    
This creates a `dist` folder whose contents should be copied into the static file server root on the DataTorrent Daemon.

Tests
-------

Tests are written using the [Mocha Framework](http://visionmedia.github.io/mocha/), the [Chai Assertion Library](http://chaijs.com/), and the [Sinon Library](http://sinonjs.org/). The `suite.js` file which is located in `ui/test/` includes all the tests to be run. The individual test files are located in the same directory of the file they each are respectively testing, and by convention end in `.spec.js`.

### Browser

To run the tests in the browser, run `make test`, then go to `http://localhost:3334/test/runner.html` to view the test results.

### CLI

To run tests via command line, run `make clitest`.


Development
-----------

The code in this project is organized in a way that is compatible with [browserify](http://browserify.org/), a tool that allows you to write projects in Common.js (node.js) conventions (e.g. `require`, `module`, `exports`). Browserify achieves this by adding a bundling step between coding and viewing in the browser, similar to a compile step for CoffeeScript. The best way to streamline this bundling step during development is with [beefy](https://github.com/chrisdickinson/beefy), which spins up a server that automatically bundles the code on request.

### Getting Started

One project goal that has not been achieved yet is to set up a complete mock implementation of the DT Gateway API to develop against, so the best way to get started developing/extending the UI is to have a Hadoop cluster with DataTorrent and the DT Gateway installed and running. The steps to set up a development environment are as follows:

1. Clone the [Malhar](https://github.com/DataTorrent/Malhar) repository to the machine that the DT Gateway is running on.
2. Create a symbolic link called dev in the static file server root folder that points to the ui/ folder in Malhar.
3. On your local machine, ensure that Malhar is also cloned, then run `make bundle` and `node server` in the ui/ folder (in two different shells or as two different background processes).
4. Navigate to the dev folder of the DT Gateway static file server URL that your local machine has access to (this may require ssh tunneling or other steps depending on your environment). This might look like: `http://node0.yourcluster.com:PORT/static/dev`.
5. the `make bundle` command starts the [beefy](https://github.com/chrisdickinson/beefy) server so that every time this URL is hit, your local code gets rebundled.

### Improving Bundling Time

Because of the size of the project, and specifically one [browserify transform](https://github.com/substack/node-browserify#list-of-source-transforms) called ktbr that precompiles templates, the bundle process can take upwards of 8 seconds. To improve this, you can run the `make bundle_no_ktbr` command, which will significantly reduce bundle time, but also relies on synchronous ajax requests to fetch template files on demand. The problem here is that fetching these files located on your local machine violates the [Same-Origin Policy](http://en.wikipedia.org/wiki/Same-origin_policy) for web browsers. To circumvent, you may run Chrome or Chromium with web security disabled:

	/path/to/chromium-or-chrome/executale/Chromium --disable-web-security


The DT Global Variable
----------------------

At runtime, the variable DT is available on the global namespace and contains all the base components that the UI relies on, such as classes that model aspects of your datatorrent applications and views that are common throughout the project. To view what is on this module, take a look at the file located at `ui/js/datatorent/index.js`.


Pages
-----

The file located at `ui/js/app/pages.js` acts as an index for all registered pages, and contains more information therein about how to set up a page. The actual page definitions are located in `ui/js/app/lib/pages/` and specify a page name, the widgets that can be instantiated, the default widget configurations (called dashboards), and the page-wide models that will be instantiated. The best way to get familiar with these is to review the code.


Widgets
-------

Widgets are simply enhanced backbone views. It is recommended (but not necessarily required) to have all widgets extend from DT.widgets.Widget, which maps to the base backbone view located at `ui/js/datatorrent/WidgetView.js`.

### Location
Widgets live in `/js/App/lib/widgets`. 

### Creation

To create a boilerplate for a widget, run 

    ./bin/createWidget [-c] _WIDGET_NAME_

This will append "Widget" to the end of your widget name, so it is not necessary to include it here. The new folder that is created will be located at `ui/js/app/lib/widgets/_WIDGET_NAME_Widget`.


Contributing
------------

If you would like to help improve this project, please follow the guidelines in the [section on contributing](https://github.com/DataTorrent/Malhar#contributing) in the Malhar repository. Additionally, for linting we use [JSHint](http://www.jshint.com/), so please run your code through this before submission.