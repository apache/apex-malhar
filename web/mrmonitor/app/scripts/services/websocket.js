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

'use strict';

angular.module('app.service')
  .provider('webSocket', function () {

    var webSocketURL;
    var webSocketObject; // for testing only

    return {
      $get: function ($q,  $rootScope, notificationService) {
        if (!webSocketURL && !webSocketObject) {
          throw 'WebSocket URL is not defined';
        }

        var socket = !webSocketObject ? new WebSocket(webSocketURL) : webSocketObject;

        var deferred = $q.defer();

        socket.onopen = function () {
          deferred.resolve();
          $rootScope.$apply();
        };

        var webSocketError = false;

        socket.onclose = function () {
          if (!webSocketError) {
            notificationService.notify({
              title: 'WebSocket Closed',
              text: 'WebSocket connection has been closed. Try refreshing the page.',
              type: 'error',
              icon: false,
              hide: false,
              history: false
            });
          }
        };

        socket.onerror = function () {
          webSocketError = true;
          notificationService.notify({
            title: 'WebSocket Error',
            text: 'WebSocket error. Try refreshing the page.',
            type: 'error',
            icon: false,
            hide: false,
            history: false
          });
        };

        var topicMap = {}; // topic -> [callbacks] mapping

        socket.onmessage = function (event) {
          var message = JSON.parse(event.data);

          var topic = message.topic;

          if (topicMap.hasOwnProperty(topic)) {
            topicMap[topic].fire(message.data);
          }
        };

        return {
          send: function (message) {
            var msg = JSON.stringify(message);

            deferred.promise.then(function () {
              console.log('send ' + msg);
              socket.send(msg);
            });
          },

          subscribe: function (topic, callback, $scope) {
            var callbacks = topicMap[topic];

            if (!callbacks) {
              var message = { type: 'subscribe', topic: topic }; // subscribe message
              this.send(message);

              callbacks = jQuery.Callbacks();
              topicMap[topic] = callbacks;
            }

            callbacks.add(callback);

            if ($scope) {
              $scope.$on('$destroy', function () {
                this.unsubscribe(topic, callback);
              }.bind(this));
            }
          },

          unsubscribe: function (topic, callback) {
            if (topicMap.hasOwnProperty(topic)) {
              var callbacks = topicMap[topic];
              callbacks.remove(callback);
            }
          }
        };
      },

      setWebSocketURL: function (wsURL) {
        webSocketURL = wsURL;
      },

      setWebSocketObject: function (wsObject) {
        webSocketObject = wsObject;
      }
    };
  });
