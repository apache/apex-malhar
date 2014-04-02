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
# should be specified by the dt.gateway.staticResourceDirectory property
# in dt-site.xml.
build:
	rm -rf dist_tmp
	mkdir dist_tmp
	cp package.json dist_tmp/package.json
	cp favicon.ico dist_tmp/favicon.ico
	mkdir dist_tmp/css
	mkdir dist_tmp/js
	mkdir dist_tmp/img
	./node_modules/.bin/lessc -ru -x --strict-imports css/index.less dist_tmp/css/index.css
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
	rm -rf dist_tmp
	mkdir dist_tmp
	cp package.json dist_tmp/package.json
	cp favicon.ico dist_tmp/favicon.ico
	mkdir dist_tmp/css
	mkdir dist_tmp/js
	mkdir dist_tmp/img
	./node_modules/.bin/lessc -ru --strict-imports css/index.less dist_tmp/css/index.css
	./node_modules/.bin/browserify -t ./node_modules/ktbr js/start.js > dist_tmp/js/bundle.js
	cp img/* dist_tmp/img/
	rm -rf dist
	mv dist_tmp dist
	node bin/setBuildVersions.js
.PHONY: test
