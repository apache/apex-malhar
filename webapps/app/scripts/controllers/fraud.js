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
        $scope.app = rest.getApp(settings.fraud.appName);

        $scope.$watch('app', function (app) {
            if (app) {
                $scope.appURL = settings.appsURL + app.id;
                $scope.startedTime = app.startedTime;
            }
        });
        
        // Options for merchant, terminal, zip, card, bin
        $scope.alertTypeTitles = {
            "smallThenLarge": "Suspicious Transaction Sequence",
            "sameCard": "Same Card Multiple Times",
            "sameBankId": "Same Bank Number Multiple Times",
            "aboveAvg": "Above Average Transaction"
        }
        $scope.stats = [
            { id: 'totalTxns',          topic: 'demos.app.frauddetect.totalTransactions', value: 0, label: 'Total Txns' },
            { id: 'amtInLastSecond',    topic: 'demos.app.frauddetect.txLastSecond',      value: 0, label: 'Total Dollars / sec' },
            // { id: 'amtInLastHour',      topic: 'demos.app.frauddetect.txLastHour',        value: 0, label: 'Total for Last Hour' },
            { id: 'avgAmtInLastSecond', topic: 'demos.app.frauddetect.avgLastSecond',     value: 0, label: 'Avg Txn Amount / sec' },
            { id: 'numFrauds',          topic: 'demos.app.frauddetect.totalFrauds',       value: 0, label: 'No. of Anomalies Detected' },
            // { id: 'avgScore',           topic: 'demos.app.frauddetect.avgScore',          value: 0, label: 'Average Score' }
        ];
        $scope.merchants = ['Wal-Mart', 'Target', 'Amazon', 'Apple', 'Sears', 'Macys', 'JCPenny', 'Levis'];
        $scope.terminals = [1, 2, 3, 4, 5, 6, 7, 8];
        $scope.zips      = [94086, 94087, 94088, 94089, 94090, 94091, 94092, 94093];
        $scope.actions   = [
            {
                id: 1,
                subtitle: $scope.alertTypeTitles.smallThenLarge,
                severity: 'low',
                description: 'This anomaly is when one credit card is used for a small purchase, then immediately again for a larger purchase. The idea here is that a scammer will first try a small purchase to ensure that the card works, then proceed with a larger purchase upon success.',
                generateTxns: function(e) {
                    
                    var bin = getRandomBin();
                    var card = getRandomCard();
                    
                    submitTransaction({ 
                        'zipCode': getRandom('zips'),
                        'merchantId': getRandom('merchants'), 
                        'terminalId': getRandom('terminals'),
                        'bankIdNum': bin,
                        'ccNum': card,
                        'amount': 5.00
                    });
                    
                    setTimeout(function() {
                        submitTransaction({ 
                            'zipCode': getRandom('zips'),
                            'merchantId': getRandom('merchants'), 
                            'terminalId': getRandom('terminals'),
                            'bankIdNum': bin,
                            'ccNum': card,
                            'amount': 600.00
                        });
                    }, 5000)
                    
                }
            },
            // {
            //     id: 2,
            //     subtitle: $scope.alertTypeTitles.sameCard,
            //     description: 'This anomaly is when one credit card is used for multiple transactions across one or more vendors within a short time interval.',
            //     generateTxns: function() {
            //         
            //         var bin = getRandomBin();
            //         var card = getRandomCard();
            //         var merchant = getRandom('merchants');
            //         
            //         var intval = setInterval(function() {
            //             submitTransaction({
            //                 'zipCode': getRandom('zips'),
            //                 'merchantId': merchant, 
            //                 'terminalId': getRandom('terminals'),
            //                 'bankIdNum': bin,
            //                 'ccNum': card,
            //                 'amount': roundToPrice(10 + Math.random() * 1000)
            //             });
            //         }, 1000);
            //         
            //         setTimeout(function() {
            //             clearInterval(intval);
            //         }, 8000);
            //     }
            // },
            {
                id: 2,
                subtitle: $scope.alertTypeTitles.sameBankId,
                severity: 'high',
                description: 'This anomaly is detected when several transactions are made by cards issued by the same bank at the same terminal ID over moving window of 30 sec.',
                generateTxns: function() {
                    var bin = getRandomBin().replace(/\d{4}$/, '1111');
                    var zipCode = getRandom('zips');
                    var terminalId = getRandom('terminals');
                    var merchant = getRandom('merchants');
                    
                    var intval = setInterval(function() {
                        submitTransaction({
                            'zipCode': zipCode,
                            'merchantId': merchant, 
                            'terminalId': terminalId,
                            'bankIdNum': bin,
                            'ccNum': getRandomCard(),
                            'amount': roundToPrice(100 + Math.random() * 10)
                        }, true);
                    }, 200);
                    
                    $.pnotify({
                        title: 'Several Transactions Being Sent',
                        text: '<strong>Bank ID:</strong> ' + bin + ', <strong>Merchant:</strong> ' + merchant,
                        type: 'success'
                    });
                    
                    setTimeout(function() {
                        clearInterval(intval);
                    }, 11000);
                }
            },
            {
                id: 3,
                subtitle: $scope.alertTypeTitles.aboveAvg,
                severity: 'medium',
                description: 'This anomaly is when a transaction at a given merchant significantly exceeds that merchant\'s average transaction amount.',
                generateTxns: function() {
                    console.log('getting randomStats');
                    // $.get('/fraud/randomStats').done(function(res) {
                    //     var bin = getRandomBin();
                    //     var merchantId = res.merchantId;
                    //     var terminalId = res.terminalId;
                    //     var zipCode = res.zipCode;
                    //     var amount = roundToPrice(res.sma + 35000);
                    //     
                    //     for (var i = 5; i >= 0; i--) {
                    //         submitTransaction({
                    //             'zipCode': zipCode,
                    //             'merchantId': merchantId, 
                    //             'terminalId': terminalId,
                    //             'bankIdNum': getRandomBin(),
                    //             'ccNum': getRandomCard(),
                    //             'amount': 20 + Math.round(Math.random() * 10)
                    //         }, true);
                    //     }
                    //     
                    //     setTimeout(function() {
                    //         submitTransaction({
                    //             'zipCode': zipCode,
                    //             'merchantId': merchantId, 
                    //             'terminalId': terminalId,
                    //             'bankIdNum': getRandomBin(),
                    //             'ccNum': getRandomCard(),
                    //             'amount': amount
                    //         });
                    //     }, 3000)
                    //     
                    // });
                    $.pnotify({
                        'type': 'info',
                        'title': 'Retrieving Average Information'
                    });
                    $.get('/fraud/randomStats').done(function(res) {

                        var bin = getRandomBin();
                        var merchantId = res.merchantId;
                        var terminalId = res.terminalId;
                        var zipCode = res.zipCode;
                        var amount = roundToPrice(res.sma + 1500);
                        
                        setTimeout(function() {
                            submitTransaction({
                                'zipCode': zipCode,
                                'merchantId': merchantId, 
                                'terminalId': terminalId,
                                'bankIdNum': getRandomBin(),
                                'ccNum': getRandomCard(),
                                'amount': amount
                            });
                        }, 1000)
                        
                    });
                }
            }
        ];
        
        // subscribe to appropriate topics for alerts and stats
        $scope.app.then(function(app) {
            socket.subscribe('demos.app.frauddetect.fraudAlert', function(res) {
                // console.log('received fraudAlert: ', res);
                // console.log(res.data.alertType, res.data.alertData ? res.data.alertData.fullCcNum : '');
                if (res.data.alertType === 'aboveAvg') {
                    console.log('aboveAvg', res.data);
                }
                if (res.data.userGenerated === "true" || res.data.userGenerated === true) {
                    displayAlert(res.data);
                }
            }, $scope);
            socket.subscribe('demos.app.frauddetect.txSummary', function(res) {
                var data = res.data;
                _.each(['amtInLastSecond','avgAmtInLastSecond','totalTxns','txnsInLastSecond'], function(key) {
                    
                    // Find stat to update
                    var stat = _.find($scope.stats, function(obj) {
                        return obj.id == key;
                    });
                    
                    // Check that stat was found
                    if (stat) {

                        if (['amtInLastSecond', 'avgAmtInLastSecond'].indexOf(key) > -1) {

                            stat.value = '$' + makeMoney(data[key]);
                            
                        } else {
                            
                            stat.value = commaGroups(data[key]);
                            
                        }
                        
                    }
                    
                });
                $scope.$apply();
            }, $scope);
        });
        $scope.stats.forEach(function(stat){
            socket.subscribe(stat.topic, function(res) {
                console.log("stat topic " + stat.topic + " data received: ", res);
                stat.value = res.value;
                $scope.$apply();
            }, $scope);
        });
        
        $scope.alerts = [];
        
        // helper function for choosing random items from a list
        function getRandom(list) {
            return $scope[list][ Math.floor(Math.random() * $scope[list].length) ];
        }
        function roundToPrice(amt) {
            // return Math.round( amt * 100 ) / 100;
            return Math.round(amt);
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
            var index = $scope.alerts.push(data) - 1;
            var alertTitle = $scope.alertTypeTitles[data.alertType];
            var html = '';
            switch(data.alertType) {
                case 'smallThenLarge': 
                    html = [
                        '<article class="alert-msg low" style="display:none">',
                            '<h1>' + alertTitle + '</h1>',
                            '<p>' + data.message + '</p>',
                            '<div><a href="#" class="btn view-txn-btn" data-txidx="' + index + '">view details</a></div>',
                        '</article>'
                    ].join('');
                break;
                case 'sameBankId':
                    console.log('same bank', data);
                    html = [
                        '<article class="alert-msg high" style="display:none">',
                            '<h1>' + alertTitle + '</h1>',
                            '<p>' + data.message + '</p>',
                            '<div><a href="#" class="btn view-txn-btn" data-txidx="' + index + '">view details</a></div>',
                        '</article>'
                    ].join('');
                break;
                default:
                    html = [
                        '<article class="alert-msg medium" style="display:none">',
                            '<h1>' + alertTitle + '</h1>',
                            '<p>' + data.message + '</p>',
                            '<div><a href="#" class="btn view-txn-btn" data-txidx="' + index + '">view details</a></div>',
                        '</article>'
                    ].join('');
                break;
                
                
            }
            var $el = $(html);
            $('#alertDisplayBox').prepend($el);
            $el
                .slideDown()
                .animate({ 'opacity': 0.5 }, 180)
                .animate({ 'opacity': 1 }, 180)
                .animate({ 'opacity': 0.5 }, 180)
                .animate({ 'opacity': 1 }, 180)
        }
        function submitTransaction(txn, noMessage) {
            socket.publish(txTopic, txn);
            console.log('txn', txn);
            if (!noMessage) {
                $.pnotify({
                    'title': 'Transaction Submitted',
                    'text': 
                        '<strong>card</strong>: ' + txn.bankIdNum + ' ' + txn.ccNum + '<br/> ' + 
                        '<strong>amount</strong>: $' + makeMoney(txn.amount) + '<br/> ' + 
                        '<strong>merchant</strong>: ' + txn.merchantId + ', <strong>terminal</strong>: ' + txn.terminalId,
        
                    'type': 'success'
                });
            }
        }
        function genTxnDisplayMarkup(alert) {
            var html = '';
            switch(alert.alertType) {
                case "smallThenLarge":
                    var info = alert.alertData;
                    html = [
                        '<strong>Card Number:</strong> ' + info.fullCcNum + '<br/>',
                        '<strong>Zip Code:</strong> ' + info.zipCode + '<br/>',
                        '<strong>Merchant:</strong> ' + info.merchantId + ' (' + info.merchantType.toLowerCase().replace('_', ' ') + ')' + '<br/>',
                        '<strong>Small Amount:</strong> $' + makeMoney(info.small) + '<br/>',
                        '<strong>Large Amount:</strong> $' + makeMoney(info.large) + '<br/>',
                        '<strong>Threshold:</strong> $'    + makeMoney(info.threshold) + '<br/>',
                        '<strong>Time:</strong> ' + new Date(1*info.time).toLocaleString() + '<br/>'
                    ].join('');
                    
                    break;
                
                case "sameBankId":
                    var info = alert.alertData;
                    html = [
                        '<strong>Transaction Count:</strong> ' + info.count + '<br/>',
                        '<strong>Bank ID Number:</strong> ' + info.bankIdNum + '<br/>',
                        '<strong>Zip Code:</strong> ' + info.zipCode + '<br/>',
                        '<strong>Merchant:</strong> ' + info.merchantId + ' (' + info.merchantType.toLowerCase().replace('_', ' ') + ')' + '<br/>',
                        '<strong>Terminal:</strong> ' + info.terminalId + '<br/>',
                        '<strong>Time:</strong> ' + new Date(1*info.time).toLocaleString() + '<br/>'
                    ].join('');
                    
                    break;
                    
                case "aboveAvg":
                    var info = alert.alertData;
                    html = [
                        '<strong>Dollar Amount:</strong> $' + makeMoney(info.amount) + '<br/>',
                        '<strong>Last Average:</strong> $' + makeMoney(info.lastSmaValue) + '<br/>',
                        '<strong>Difference:</strong> $' + makeMoney(info.change) + '<br/>',
                        '<strong>Zip Code:</strong> ' + info.zipCode + '<br/>',
                        '<strong>Merchant:</strong> ' + info.merchantId + ' (' + info.merchantType.toLowerCase().replace('_', ' ') + ')' + '<br/>',
                        '<strong>Terminal:</strong> ' + info.terminalId + '<br/>',
                        '<strong>Time:</strong> ' + new Date(1*info.time).toLocaleString() + '<br/>'
                    ].join('');
                    break;
            }
            return html;
        }
        function makeMoney(value) {
            value = (value * 1).toFixed(2);
            return commaGroups(value);
        }
        function commaGroups(value) {
            var parts = value.toString().split(".");
            parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
            return parts.join(".");
        }

        var scopeDestroyed = false;

        function  updateFraudCount() {
            if (scopeDestroyed) {
                return;
            }

            if ($scope.startedTime) {
                $.get('/fraud/alertCount?since=' + $scope.startedTime).done(function(res) {
                
                    var countStat = _.find($scope.stats, function(obj) {
                        return obj.id == 'numFrauds';
                    });
                
                    var value = _.reduce(res, function(memo, val, key) { return memo + val }, 0);
                
                    countStat.value = commaGroups(value); 
                
                    setTimeout(updateFraudCount, 2000);
                });
            } else {
                setTimeout(updateFraudCount, 2000);
            }
        }
        
        // Set up viewing transaction modal
        $('#alertDisplayBox').on('click', '.view-txn-btn', function(evt) {
            evt.preventDefault();
            var index = $(this).attr('data-txidx');
            var alert = $scope.alerts[index];
            $('#txn-modal .modal-body').html(genTxnDisplayMarkup(alert));
            $('#txn-modal').modal();
        });
        
        // Start interval to poll for alerts count
        updateFraudCount();
        
        $scope.clearFrauds = function() {
            $('#alertDisplayBox').html("");
        }

        // stop server polling on destroy (e.g. when navigating to another view)
        $scope.$on('$destroy', function () {
          scopeDestroyed = true;
        }.bind(this));
    }]);

})();
