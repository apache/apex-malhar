/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var fs = require('fs');
var path = require('path');

// Read the index.build.html file into memory
var index_markup = fs.readFileSync(path.normalize(__dirname + '/../dist/index.html'), 'utf8');

// Read package.json
var pkg_string = fs.readFileSync(path.normalize(__dirname + '/../dist/package.json'), 'utf8');
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
fs.renameSync(path.normalize(__dirname + '/../dist/css/index.css'), path.normalize(__dirname + '/../dist/css/' + css_filename));
fs.renameSync(path.normalize(__dirname + '/../dist/js/bundle.js'), path.normalize(__dirname + '/../dist/js/' + js_filename));

// Replace the content of dist/index.html
fs.writeFileSync(path.normalize(__dirname + '/../dist/index.html'), index_markup);