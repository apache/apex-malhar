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
function like(term, value, computedValue, row) {
    var
    term = term.toLowerCase(),
    value = value.toLowerCase(),
    negate = term.slice(0,1) === '!';
    
    if (negate) {
        term = term.substr(1);
        if (term === '') {
            return true;
        }
        return value.indexOf(term) === -1;
    }
    
    return value.indexOf(term) > -1;
}
function likeFormatted(term, value, computedValue, row) {
    return like(term,computedValue,computedValue, row);
}
function is(term, value, computedValue, row) {
    term = term.toLowerCase();
    value = value.toLowerCase();
    return term == value;
}
function number(term, value) {
    value *= 1;
    var first_two = term.substr(0,2);
    var first_char = term[0];
    var against_1 = term.substr(1)*1;
    var against_2 = term.substr(2)*1;
    if ( first_two == "<=" ) return value <= against_2 ;
    if ( first_two == ">=" ) return value >= against_2 ;
    if ( first_char == "<" ) return value < against_1 ;
    if ( first_char == ">" ) return value > against_1 ;
    if ( first_char == "~" ) return Math.round(value) == against_1 ;
    if ( first_char == "=" ) return against_1 == value ;
    return value.toString().indexOf(term.toString()) > -1 ;
}
function numberFormatted(term, value, computedValue) {
    return number(term, computedValue);
}
var unitmap = {
    "second": 1000,
    "minute": 60000,
    "hour": 3600000,
    "day": 86400000,
    "week": 86400000*7,
    "month": 86400000*31,
    "year": 86400000*365
}
function parseDateFilter(string) {
    
    // split on clauses (if any)
    var clauses = string.split(",");
    var total = 0;
    
    // parse each clause
    for (var i = 0; i < clauses.length; i++) {
        var clause = clauses[i].trim();
        var terms = clause.split(" ");
        if (terms.length < 2) continue;
        var count = terms[0]*1;
        var unit = terms[1].replace(/s$/, '');
        if (! unitmap.hasOwnProperty(unit) ) continue;
        total += count * unitmap[unit];
    };
    
    return total;
    
}
function date(term, value) {
    // < 1 day ago
    // < 10 minutes ago
    // < 10 minutes, 50 seconds ago
    // > 2 days ago
    // >= 1 day ago
    
    value *= 1;
    var now = (+new Date());
    var first_two = term.substr(0,2);
    var first_char = term[0];
    var against_1 = (term.substr(1)).trim();
    var against_2 = (term.substr(2)).trim();
    if ( first_two == "<=" ) {
        var lowerbound = now - parseDateFilter(against_2);
        return value >= lowerbound;
    }
    else if ( first_two == ">=" ) {
        var upperbound = now - parseDateFilter(against_2);
        return value <= upperbound;
    }
    else if ( first_char == "<" ) {
        var lowerbound = now - parseDateFilter(against_1);
        return value > lowerbound;
    }
    else if ( first_char == ">" ) {
        var upperbound = now - parseDateFilter(against_1);
        return value < upperbound;
    } else {
        // no comparative signs found
        return false;
    }
}

exports.like = like;
exports.likeFormatted = likeFormatted;
exports.is = is;
exports.number = number;
exports.numberFormatted = numberFormatted;
exports.date = date;