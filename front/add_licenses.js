var _ = require('underscore');
var fs = require('fs');
var license_string = '/*\n' +
 '* Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.\n' +
 '*\n' +
 '* Licensed under the Apache License, Version 2.0 (the "License");\n' +
 '* you may not use this file except in compliance with the License.\n' +
 '* You may obtain a copy of the License at\n' +
 '*\n' +
 '*   http://www.apache.org/licenses/LICENSE-2.0\n' +
 '*\n' +
 '* Unless required by applicable law or agreed to in writing, software\n' +
 '* distributed under the License is distributed on an "AS IS" BASIS,\n' +
 '* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n' +
 '* See the License for the specific language governing permissions and\n' +
 '* limitations under the License.\n' +
 '*/\n';

var license_re = new RegExp('Licensed under the Apache License');

function doneFn (err, list) {

  if (err) {
    console.log('An error occurred');
  } else {
    var jsfiles = _.filter(list, function(item) {
      return /\.js$/.test(item);
    });
    _.each(jsfiles, function(filepath) {
      var contents = fs.readFileSync(filepath);
      if (!license_re.test(contents)) {
        fs.writeFileSync(filepath, license_string + contents);
        console.log('added license headers to: ' + filepath);
      } else {
        console.log(filepath + ' already has license headers.');
      }
    });
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

walk(__dirname + '/js/app', doneFn);
walk(__dirname + '/js/datatorrent', doneFn);