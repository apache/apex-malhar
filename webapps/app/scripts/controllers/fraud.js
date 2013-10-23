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

/*global angular, jQuery, _*/
(function () {
'use strict';

angular.module('fraud')

    .controller('FraudController', ['$scope', 'rest', 'socket', function ($scope, rest, socket) {
        
        // topic for publishing transactions
        var txTopic = 'demos.app.frauddetect.submitTransaction';

        $scope.appId = rest.getAppId(settings.fraud.appName);

        $scope.$watch('appId', function (appId) {
            if (appId) {
                $scope.appURL = settings.appsURL + appId;
            }
        });
        
        // Options for merchant, terminal, zip, card, bin
        $scope.alertTypeTitles = {
            "smallThenLarge": "Suspicious Transaction Sequence",
            "sameCard": "Same Card Multiple Times",
            "sameBankId": "Same Bank Number Multiple Times",
            "aboveAvg": "Above Average Transaction"
        }
        $scope.merchants = ['Wal-Mart', 'Target', 'Amazon', 'Apple', 'Sears', 'Macys', 'JCPenny', 'Levis'];
        $scope.terminals = [1, 2, 3, 4, 5, 6, 7, 8];
        $scope.zips      = [94086, 94087, 94088, 94089, 94090, 94091, 94092, 94093]
        $scope.actions   = [
            {
                id: 1,
                subtitle: $scope.alertTypeTitles.smallThenLarge,
                description: 'This anomaly is when one credit card is used for a small purchase, then immediately again for a larger purchase. The idea here is that a scammer will first try a small purchase to ensure that the card works, then proceed with a larger purchase upon success.',
                generateTxns: function(e) {
                    
                    var bin = getRandomBin();
                    var card = getRandomCard();
                    
                    socket.publish( txTopic, { 
                        'zipCode': getRandom('zips'),
                        'merchantId': getRandom('merchants'), 
                        'terminalId': getRandom('terminals'),
                        'bankIdNum': bin,
                        'ccNum': card,
                        'amount': 5.00
                    });
                    
                    setTimeout(function() {
                        socket.publish( txTopic, { 
                            'zipCode': getRandom('zips'),
                            'merchantId': getRandom('merchants'), 
                            'terminalId': getRandom('terminals'),
                            'bankIdNum': bin,
                            'ccNum': card,
                            'amount': 44000.00
                        });
                    }, 5000)
                    
                }
            },
            {
                id: 2,
                subtitle: $scope.alertTypeTitles.sameCard,
                description: 'This anomaly is when one credit card is used for multiple transactions across one or more vendors within a short time interval.',
                generateTxns: function() {
                    
                    var bin = getRandomBin();
                    var card = getRandomCard();
                    
                    var intval = setInterval(function() {
                        socket.publish(txTopic, {
                            'zipCode': getRandom('zips'),
                            'merchantId': getRandom('merchants'), 
                            'terminalId': getRandom('terminals'),
                            'bankIdNum': bin,
                            'ccNum': card,
                            'amount': roundToPrice(10 + Math.random() * 1000)
                        });
                    }, 1000);
                    
                    setTimeout(function() {
                        clearInterval(intval);
                    }, 8000);
                }
            },
            {
                id: 3,
                subtitle: $scope.alertTypeTitles.sameBankId,
                description: 'This anomaly is when several transactions are made with cards sharing the same Bank Identification Number (first 12 digits). An employee at a bank may use this tactic to attempt fraud.',
                generateTxns: function() {
                    var bin = getRandomBin();
                    
                    var intval = setInterval(function() {
                        socket.publish(txTopic, {
                            'zipCode': getRandom('zips'),
                            'merchantId': getRandom('merchants'), 
                            'terminalId': getRandom('terminals'),
                            'bankIdNum': bin,
                            'ccNum': getRandomCard(),
                            'amount': roundToPrice(10 + Math.random() * 1000)
                        });
                    }, 100);
                    
                    setTimeout(function() {
                        clearInterval(intval);
                    }, 8000);
                }
            },
            {
                id: 4,
                subtitle: $scope.alertTypeTitles.aboveAvg,
                description: 'This anomaly is when a transaction at a given merchant significantly exceeds that merchant\'s average transaction amount.',
                generateTxns: function() {
                    var bin = getRandomBin();
                    
                    var intval = setInterval(function() {
                        socket.publish(txTopic, {
                            'zipCode': getRandom('zips'),
                            'merchantId': getRandom('merchants'), 
                            'terminalId': getRandom('terminals'),
                            'bankIdNum': getRandomBin(),
                            'ccNum': getRandomCard(),
                            'amount': roundToPrice(10000 + Math.random() * 1000)
                        });
                    }, 1000);
                    
                    setTimeout(function() {
                        clearInterval(intval);
                    }, 10000);
                }
            }
        ];
        
        // subscribe to appropriate topic for alerts
        $scope.appId.then(function(appId) {
            socket.subscribe('demos.app.frauddetect.fraudDetect', function(res) {
                if (res.data.userGenerated === "true" || res.data.userGenerated === true) {
                    displayAlert(res.data);
                }
            });
        });
        
        $scope.alerts = [];
        
        // helper function for choosing random items from a list
        function getRandom(list) {
            return $scope[list][ Math.floor(Math.random() * $scope[list].length) ];
        }
        function roundToPrice(amt) {
            return Math.round( amt * 100 ) / 100;
        }
        function getRandomBin() {
            // Bank ID will be between 1000 0000 and 3500 0000 (25 BINs)
            var base = Math.floor(Math.random() * 25) + 10;
            return base + "00 0000";
        }
        function getRandomCard() {
            // CC will be 1000 0000 to 1400 0000 (400,000 cards per BIN)
            var base = Math.floor(Math.random() * 400000) + 10000000;
            var baseString = base + '';
            return baseString.substring(0, 4) + " " + baseString.substring(4);
        }
        function displayAlert(data) {
            var alertTitle = $scope.alertTypeTitles[data.alertType];
            var html = [
                '<article class="alert-msg medium" style="display:none">',
                    '<h1>' + alertTitle + '</h1>',
                    '<p>' + data.message + '</p>',
                '</article>'
            ].join('');
            var $el = $(html);
            $('#alertDisplayBox').prepend($el);
            $el
                .slideDown()
                .animate({ 'opacity': 0.5 }, 180)
                .animate({ 'opacity': 1 }, 180)
                .animate({ 'opacity': 0.5 }, 180)
                .animate({ 'opacity': 1 }, 180)
        }
    }]);

})();
