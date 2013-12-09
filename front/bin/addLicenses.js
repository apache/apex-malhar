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
var _ = require('underscore');
var path = require('path');
var fs = require('fs');

var license_string = fs.readFileSync(__dirname + '/license_header.txt') + '\n';

var license_re = new RegExp('Licensed under ' + 'the Apache License');

function doneFn (err, list) {

  if (err) {
    console.log('An error occurred');
  } else {
    var jsfiles = _.filter(list, function(item) {
      return /\.js$/.test(item);
    });
    var already = 0;
    _.each(jsfiles, function(filepath) {
      var contents = fs.readFileSync(filepath);
      if (!license_re.test(contents)) {
        fs.writeFileSync(filepath, license_string + contents);
        console.log('added license headers to: ' + filepath);
      } else {
        already++;
      }
    });

    console.log(already + ' files already had license headers');
  }

}

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

walk(path.normalize(__dirname + '/../js/app'), doneFn);
walk(path.normalize(__dirname + '/../js/datatorrent'), doneFn);
walk(__dirname, doneFn);
console.log('__dirname', __dirname);