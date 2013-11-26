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
// var _ = require('underscore');
// var BigRat = require('big-rational');
var BigInteger = require('jsbn');

exports.transformLiveTuple = function(tuple) {
    var idxBI = new BigInteger(tuple.tupleCount+'');
    tuple.idx = idxBI.subtract(BigInteger.ONE).toString();
    delete tuple.tupleCount;
    return tuple
}
exports.scaleFraction = function(srcNumerator, srcDenom, trgDenom, round) {
    // srcNumerator / srcDenom = x / trgDenom, returns x
    // var result = BigRat( srcNumerator+'' ).times( trgDenom+'' ).over( srcDenom+'' );
    
    if (srcDenom === 0) throw new Error('The source denominator cannot be zero.');
    
    var result = (srcNumerator * trgDenom)/srcDenom;
    // if (round) result = result.round();
    // return result.toDecimal();
    
    if (round) result = Math.round(result);
    return result;
}