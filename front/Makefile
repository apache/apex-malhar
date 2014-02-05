# Runs tests in the browser, on demand. Refreshing the 
# runner.html page in this case will retest the code
test:
	# To view browser tests, go to http://localhost:3334/test/runner.html
	./node_modules/.bin/beefy test/suite.js:test.bundle.js 3334 -- -t ktbr --debug

# Runs tests directly in the terminal using the mocha-phantomjs
# module. This is used for continuous integration.
clitest:
	./node_modules/.bin/browserify test/suite.js -t ktbr > test/suite.bundle.js
	./node_modules/.bin/mocha-phantomjs test/runner-cli.html
	rm test/suite.bundle.js

# The build step. This creates a dist folder whose contents should be placed 
# in the static file directory of the DataTorrent Gateway. This directory
# should be specified by the stram.gateway.staticResourceDirectory property
# in stram-site.xml.
build:
	rm -rf dist_tmp
	mkdir dist_tmp
	cp package.json dist_tmp/package.json
	cp favicon.ico dist_tmp/favicon.ico
	mkdir dist_tmp/css
	mkdir dist_tmp/js
	mkdir dist_tmp/img
	./node_modules/.bin/lessc -x --strict-imports css/index.less dist_tmp/css/index.css
	./node_modules/.bin/browserify -t ./node_modules/ktbr js/start.js > dist_tmp/js/bundle.js
	./node_modules/.bin/uglifyjs \
		js/vendor/jquery/dist/jquery.js \
		dist_tmp/js/bundle.js \
		-o dist_tmp/js/bundle.js
	cp img/* dist_tmp/img/
	rm -rf dist
	mv dist_tmp dist
	node bin/setBuildVersions.js

# Same as above command, only it does not minify anything and keeps the console
# logger enabled. Good for debugging issues that occur when the project is
# deployed but not during development.
build_debug:
	mkdir dist_tmp
	cp index.html dist_tmp/index.html
	cp package.json dist_tmp/package.json
	cp favicon.ico dist_tmp/favicon.ico
	mkdir dist_tmp/css
	mkdir dist_tmp/js
	mkdir dist_tmp/img
	./node_modules/.bin/lessc -x --strict-imports css/index.less dist_tmp/css/index.css
	./node_modules/.bin/browserify -t ./node_modules/ktbr js/start.js > dist_tmp/js/bundle.js
	cat js/vendor/jquery/dist/jquery.js dist_tmp/js/bundle.js > dist_tmp/js/bundle.tmp.js
	mv dist_tmp/js/bundle.tmp.js dist_tmp/js/bundle.js
	cp img/* dist_tmp/img/
	rm -rf dist
	mv dist_tmp dist
	node bin/setBuildVersions.js

# Spins up a beefy server that will browserify the UI javascript on-demand.
# Used only during development. 
bundle:
	./node_modules/.bin/beefy js/start.dev.js:bundle.js 9222 -- --ig --debug

# Same as above, but uses the ktbr transformation to inline all underscore templates
bundle_ktbr:
	./node_modules/.bin/beefy js/start.dev.js:bundle.js 9222 -- -t ktbr --debug


# This bundle will trigger the page to refresh when a change to any files have been made.
# It can be convenient, but can also start hogging the CPU.
livebundle:
	./node_modules/.bin/beefy js/start.dev.js:bundle.js 9222 --live 9223 -- --ig --debug

.PHONY: test
