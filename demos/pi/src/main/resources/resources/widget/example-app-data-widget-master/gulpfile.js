'use strict';

var gulp = require('gulp'),
    less = require('gulp-less'),
    usemin = require('gulp-usemin'),
    angularFilesort = require('gulp-angular-filesort'),
    bowerFiles = require('main-bower-files'),
    http = require('http'),
    connect = require('gulp-connect'),
    concat = require('gulp-concat'),
    merge = require('merge-stream'),
    inject = require('gulp-inject');

gulp.task('less', function() {
  gulp.src('./app/**/*.less')
    .pipe(less({
      paths: [
        'bower_components',
        './app',
        './components'
      ]
    }))
    .pipe(gulp.dest('./dist/styles'))
    .pipe(connect.reload());
});

gulp.task('watch', ['server'], function() {
  gulp.watch('app/**/*.less', ['less']);
  gulp.watch(['app/**/*.js', 'app/*.js', 'app/index.html'], ['usemin']);
});

gulp.task('inject', function () {

  var target = gulp.src('./app/index.html');
  var sources = gulp.src(['./app/**/*.js', './dist/**/*.css'], {read: false}).pipe(angularFilesort());

  return target
    .pipe(inject(gulp.src(bowerFiles(), {read: false}), {name: 'bower'}))
    .pipe(inject(sources, { ignorePath: 'dist' }))
    .pipe(gulp.dest('./dist'));
});

gulp.task('usemin', ['inject'], function() {

  return gulp.src('./dist/index.html')
    .pipe(usemin({
      vendor: ['concat'],
      js: ['concat']
    }))
    .pipe(gulp.dest('./dist'))
    .pipe(connect.reload());
});

gulp.task('server', function() {
  connect.server({
    livereload: true,
    root: ['dist','bower_components/datatorrent-console-package/dist'],
    port: 8080
  });
});

gulp.task('build', function() {
  var jsFile = gulp.src('app/src/**/*.js')
    .pipe(angularFilesort())
    .pipe(concat('widget.js'))
    .pipe(gulp.dest('build'));

  var cssFile = gulp.src('app/src/widget.less')
    .pipe(less())
    .pipe(gulp.dest('build'));

  return merge(jsFile, cssFile);

});