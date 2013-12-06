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

/**
 * Scans the js folder for all html files, 
 * then compiles them into template functions
 * and stores them in js/precompiled-templates.js.
 */

var _ = require('underscore');
var fs = require('fs');

// The starting content for precompiled-templates.js
var result = 'var _ = require(\'underscore\'); exports = module.exports = {';

// Walks a directory and gets an array of filenames
function walk (dir, done) {
  var results = [];
  fs.readdir(dir, function(err, list) {
    if (err) return done(err);
    var pending = list.length;
    if (!pending) return done(null, results);
    list.forEach(function(file) {
      file = dir + '/' + file;
      fs.stat(file, function(err, stat) {
        if (stat && stat.isDirectory()) {
          walk(file, function(err, res) {
            results = results.concat(res);
            if (!--pending) done(null, results);
          });
        } else {
          results.push(file);
          if (!--pending) done(null, results);
        }
      });
    });
  });
};

// Walk the js directory
walk('./js', function(err, res) {

  // filter for only html files
  var html_files = _.filter(res, function(filename) {
    return /\.html$/.test(filename);
  });

  _.each( html_files, function(path) {

    // Get the contents
    var html = fs.readFileSync(path, 'utf8');

    // Remove the ./js prefix, so that the 
    // path is relative to start.dev.js
    var key = path.replace(/^\.\/js/, '');

    // Create the function string
    var fn_string = _.template(html).source;

    // Append to result
    result += '"' + key + '": ' + fn_string + ',';

  });

  result = result.replace(/,$/, '');
  result += '};';

  fs.writeFileSync('./js/precompiled-templates.js', result);

});
