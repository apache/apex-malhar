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
var ejs = require('ejs');

// Read package.json
var pkg = require('../package.json');
var version = pkg.version;

// Replace appropriate spots
index_markup = ejs.renderFile(path.normalize(__dirname + '/../views/index.build.ejs'), {
    locals: pkg
}, function(err, res) {
    fs.writeFileSync(path.normalize(__dirname + '/../dist/index.html'), res);
});

// Rename the index.css and bundle.js files
fs.renameSync(path.normalize(__dirname + '/../dist/css/index.css'), path.normalize(__dirname + '/../dist/css/index.' + version + '.css'));
fs.renameSync(path.normalize(__dirname + '/../dist/js/bundle.js'), path.normalize(__dirname + '/../dist/js/bundle.' + version + '.js'));
console.log('version: ', version);