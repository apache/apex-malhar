var fs = require('fs');

// Read the index.build.html file into memory
var index_markup = fs.readFileSync('./dist/index.html', 'utf8');

// Read package.json
var pkg_string = fs.readFileSync('./dist/package.json');
var pkg = JSON.parse(pkg_string);
var version = pkg.version;

// Set the names of the new files
var css_filename = 'index.' + version + '.css';
var js_filename = 'bundle.' + version + '.js';
var css_replacement = 'href="css/' + css_filename + '"';
var js_replacement = 'src="js/' + js_filename + '"';

// Create the RegExp objects
var cssRE = /href="css\/index.css"/
var jsRE = /src="js\/bundle.js"/;

// Check that this is still compatible
if ( ! (cssRE.test(index_markup)) || ! (jsRE.test(index_markup)) ) {
    throw new Error('index.built.html is no longer in an expected format');
}

// Replace appropriate spots
index_markup = index_markup.replace(cssRE, css_replacement).replace(jsRE, js_replacement);

// Rename the index.css and bundle.js files
fs.renameSync('./dist/css/index.css', './dist/css/' + css_filename);
fs.renameSync('./dist/js/bundle.js', './dist/js/' + js_filename);

// Replace the content of dist/index.html
fs.writeFileSync('./dist/index.html', index_markup);