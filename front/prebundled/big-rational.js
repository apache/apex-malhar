require=(function(e,t,n){function i(n,s){if(!t[n]){if(!e[n]){var o=typeof require=="function"&&require;if(!s&&o)return o(n,!0);if(r)return r(n,!0);throw new Error("Cannot find module '"+n+"'")}var u=t[n]={exports:{}};e[n][0].call(u.exports,function(t){var r=e[n][1][t];return i(r?r:t)},u,u.exports)}return t[n].exports}var r=typeof require=="function"&&require;for(var s=0;s<n.length;s++)i(n[s]);return i})({"./node_modules/big-rational":[function(require,module,exports){
module.exports=require('SKeR2S');
},{}],"SKeR2S":[function(require,module,exports){


var bigRat = (function () {
    if (typeof require !== "undefined") {
        bigInt = require("big-integer");
    }
    function gcd(a, b) {
        if(b.equals(0)) {
            return a;
        }
        return gcd(b, a.mod(b));
    }
    function lcm(a, b) {
        return a.times(b).divide(gcd(a, b));
    }
    function create(numerator, denominator, preventReduce) {
        denominator = denominator || bigInt(1);
        preventReduce = preventReduce || false;
        var obj = {
            numerator: numerator,
            denominator: denominator,
            num: numerator,
            denom: denominator,
            reduce: function () {
                var divisor = gcd(obj.num, obj.denom);
                var num = obj.num.divide(divisor);
                var denom = obj.denom.divide(divisor);
                if(denom.lesser(0)) {
                    num = num.times(-1);
                    denom = denom.times(-1);
                }
                if(denom.equals(0)) {
                    throw "Denominator cannot be 0.";
                }
                return create(num, denom, true);
            },
            abs: function () {
                if (obj.isPositive()) return obj;
                return obj.negate();
            },
            multiply: function (n, d) {
                n = interpret(n, d);
                return create(obj.num.times(n.num), obj.denom.times(n.denom));
            },
            times: function (n, d) {
                return obj.multiply(n, d);
            },
            divide: function (n, d) {
                n = interpret(n, d);
                return create(obj.num.times(n.denom), obj.denom.times(n.num));
            },
            over: function (n, d) {
                return obj.divide(n, d);
            },
            mod: function (n, d) {
                var n = interpret(n, d);
                return obj.minus(n.times(obj.over(n).floor()));
            },
            add: function (n, d) {
                n = interpret(n, d);
                var multiple = lcm(obj.denom, n.denom);
                var a = multiple.divide(obj.denom);
                var b = multiple.divide(n.denom);

                a = obj.num.times(a);
                b = n.num.times(b);
                return create(a.add(b), multiple);
            },
            plus: function (n, d) {
                return obj.add(n, d);
            },
            negate: function () {
                var num = bigInt.zero.minus(obj.num);
                return create(num, obj.denom);
            },
            subtract: function (n, d) {
                n = interpret(n, d);
                return obj.add(n.negate());
            },
            minus: function (n, d) {
                return obj.subtract(n, d);
            },
            isPositive: function () {
                return obj.num.isPositive();
            },
            isNegative: function () {
                return !obj.isPositive();
            },
            isZero: function () {
                return obj.equals(0, 1);
            },
            compare: function (n, d) {
                n = interpret(n, d);
                if(obj.num.equals(n.num) && obj.denom.equals(n.denom)) {
                    return 0;
                }
                var newDenom = obj.denom.times(n.denom);
                var comparison = newDenom.greater(0) ? 1 : -1;
                if(obj.num.times(n.denom).greater(n.num.times(obj.denom))) {
                    return comparison;
                } else {
                    return -comparison;
                }
            },
            equals: function (n, d) {
                return obj.compare(n, d) === 0;
            },
            notEquals: function (n, d) {
                return !obj.equals(n, d);
            },
            lesser: function (n, d) {
                return obj.compare(n, d) < 0;
            },
            lesserOrEquals: function (n, d) {
                return obj.compare(n, d) <= 0;
            },
            greater: function (n, d) {
                return obj.compare(n, d) > 0;
            },
            greaterOrEquals: function (n, d) {
                return obj.compare(n, d) >= 0;
            },
            floor: function (toBigInt) {
                var floor = obj.num.over(obj.denom);
                if(toBigInt) {
                    return floor;
                }
                return create(floor);
            },
            ceil: function (toBigInt) {
                var div = obj.num.divmod(obj.denom);
                var ceil;

                ceil = div.quotient;
                if(div.remainder.notEquals(0)) {
                    ceil = ceil.add(1);
                }
                if(toBigInt) {
                    return ceil;
                }
                return create(ceil);
            },
            round: function (toBigInt) {
                return obj.add(1, 2).floor(toBigInt);
            },
            toString: function () {
                var o = obj.reduce();
                return o.num.toString() + "/" + o.denom.toString();
            },
            valueOf: function() {
                return obj.num / obj.denom;
            },
            toDecimal: function (digits) {
                digits = digits || 10;
                var n = obj.num.divmod(obj.denom);
                var intPart = n.quotient.toString();
                var remainder = parse(n.remainder, obj.denom);
                var decPart = "";
                while(decPart.length <= digits) {
                    var i;
                    for(i = 0; i < 10; i++) {
                        if(parse(decPart + i, "1" + Array(decPart.length + 2).join("0")).greater(remainder)) {
                            i--;
                            break;
                        }
                    }
                    decPart += i;
                }
                while(decPart.slice(-1) === "0") {
                    decPart = decPart.slice(0, -1);
                }
                if(decPart === "") {
                    return intPart;
                }
                return intPart + "." + decPart;
            }
        };
        return preventReduce ? obj : obj.reduce();
    }
    function interpret(n, d) {
        return parse(n, d);
    }
    function parseDecimal(n) {
        var parts = n.split("e");
        if(parts.length > 2) {
            throw new Error("Invalid input: too many 'e' tokens");
        }
        if(parts.length > 1) {
            var isPositive = true;
            if(parts[1][0] === "-") {
                parts[1] = parts[1].slice(1);
                isPositive = false;
            }
            if(parts[1][0] === "+") {
                parts[1] = parts[1].slice(1);
            }
            var significand = parseDecimal(parts[0]);
            var exponent = create(bigInt(10).pow(parts[1]));
            if(isPositive) {
                return significand.times(exponent);
            } else {
                return significand.over(exponent);
            }
        }
        parts = n.split(".");
        if(parts.length > 2) {
            throw new Error("Invalid input: too many '.' tokens");
        }
        if(parts.length > 1) {
            var intPart = create(bigInt(parts[0]));
            var length = parts[1].length;
            while(parts[1][0] === "0") {
                parts[1] = parts[1].slice(1);
            }
            var exp = "1" + Array(length + 1).join("0");
            var decPart = create(bigInt(parts[1]), bigInt(exp));
            return intPart.add(decPart);
        }
        return create(bigInt(n));
    }
    function parse(a, b) {
        if(!a) {
            return create(bigInt(0));
        }
        if(b) {
            return create(bigInt(a), bigInt(b));
        }
        if(typeof a === "object") {
            if(a.instanceofBigInt) {
                return create(a);
            }
            return a;
        }
        var num;
        var denom;

        var text = a + "";
        var texts = text.split("/");
        if(texts.length > 2) {
            throw new Error("Invalid input: too many '/' tokens");
        }
        if(texts.length > 1) {
            var parts = texts[0].split("_");
            if(parts.length > 2) {
                throw new Error("Invalid input: too many '_' tokens");
            }
            if(parts.length > 1) {
                var isPositive = parts[0][0] !== "-";
                num = bigInt(parts[0]).times(texts[1]);
                if(isPositive) {
                    num = num.add(parts[1]);
                } else {
                    num = num.subtract(parts[1]);
                }
                denom = bigInt(texts[1]);
                return create(num, denom).reduce();
            }
            return create(bigInt(texts[0]), bigInt(texts[1]));
        }
        return parseDecimal(text);
    }

    parse.zero = parse(0);
    parse.one = parse(1);
    parse.minusOne = parse(-1);

    return parse;
})();
if (typeof module !== "undefined") {
    if (module.hasOwnProperty("exports")) {
        module.exports = bigRat;
    }
}
},{"big-integer":1}],1:[function(require,module,exports){
﻿var bigInt = (function () {
    var base = 10000000, logBase = 7;
    var sign = {
        positive: false,
        negative: true
    };

    var normalize = function (first, second) {
        var a = first.value, b = second.value;
        var length = a.length > b.length ? a.length : b.length;
        for (var i = 0; i < length; i++) {
            a[i] = a[i] || 0;
            b[i] = b[i] || 0;
        }
        for (var i = length - 1; i >= 0; i--) {
            if (a[i] === 0 && b[i] === 0) {
                a.pop();
                b.pop();
            } else break;
        }
        if (!a.length) a = [0], b = [0];
        first.value = a;
        second.value = b;
    };

    var parse = function (text, first) {
        if (typeof text === "object") return text;
        text += "";
        var s = sign.positive, value = [];
        if (text[0] === "-") {
            s = sign.negative;
            text = text.slice(1);
        }
        var text = text.split("e");
        if (text.length > 2) throw new Error("Invalid integer");
        if (text[1]) {
            var exp = text[1];
            if (exp[0] === "+") exp = exp.slice(1);
            exp = parse(exp);
            if (exp.lesser(0)) throw new Error("Cannot include negative exponent part for integers");
            while (exp.notEquals(0)) {
                text[0] += "0";
                exp = exp.prev();
            }
        }
        text = text[0];
        if (text === "-0") text = "0";
        var isValid = /^([0-9][0-9]*)$/.test(text);
        if (!isValid) throw new Error("Invalid integer");
        while (text.length) {
            var divider = text.length > logBase ? text.length - logBase : 0;
            value.push(+text.slice(divider));
            text = text.slice(0, divider);
        }
        var val = bigInt(value, s);
        if (first) normalize(first, val);
        return val;
    };

    var goesInto = function (a, b) {
        var a = bigInt(a, sign.positive), b = bigInt(b, sign.positive);
        if (a.equals(0)) throw new Error("Cannot divide by 0");
        var n = 0;
        do {
            var inc = 1;
            var c = bigInt(a.value, sign.positive), t = c.times(10);
            while (t.lesser(b)) {
                c = t;
                inc *= 10;
                t = t.times(10);
            }
            while (c.lesserOrEquals(b)) {
                b = b.minus(c);
                n += inc;
            }
        } while (a.lesserOrEquals(b));

        return {
            remainder: b.value,
            result: n
        };
    };

    var bigInt = function (value, s) {
        var self = {
            value: value,
            sign: s
        };
        var o = {
            value: value,
            sign: s,
            negate: function (m) {
                var first = m || self;
                return bigInt(first.value, !first.sign);
            },
            abs: function (m) {
                var first = m || self;
                return bigInt(first.value, sign.positive);
            },
            add: function (n, m) {
                var s, first = self, second;
                if (m) (first = parse(n)) && (second = parse(m));
                else second = parse(n, first);
                s = first.sign;
                if (first.sign !== second.sign) {
                    first = bigInt(first.value, sign.positive);
                    second = bigInt(second.value, sign.positive);
                    return s === sign.positive ?
						o.subtract(first, second) :
						o.subtract(second, first);
                }
                normalize(first, second);
                var a = first.value, b = second.value;
                var result = [],
					carry = 0;
                for (var i = 0; i < a.length || carry > 0; i++) {
                    var sum = (a[i] || 0) + (b[i] || 0) + carry;
                    carry = sum >= base ? 1 : 0;
                    sum -= carry * base;
                    result.push(sum);
                }
                return bigInt(result, s);
            },
            plus: function (n, m) {
                return o.add(n, m);
            },
            subtract: function (n, m) {
                var first = self, second;
                if (m) (first = parse(n)) && (second = parse(m));
                else second = parse(n, first);
                if (first.sign !== second.sign) return o.add(first, o.negate(second));
                if (first.sign === sign.negative) return o.subtract(o.negate(second), o.negate(first));
                if (o.compare(first, second) === -1) return o.negate(o.subtract(second, first));
                var a = first.value, b = second.value;
                var result = [],
					borrow = 0;
                for (var i = 0; i < a.length; i++) {
                    a[i] -= borrow;
                    borrow = a[i] < b[i] ? 1 : 0;
                    var minuend = (borrow * base) + a[i] - b[i];
                    result.push(minuend);
                }
                return bigInt(result, sign.positive);
            },
            minus: function (n, m) {
                return o.subtract(n, m);
            },
            multiply: function (n, m) {
                var s, first = self, second;
                if (m) (first = parse(n)) && (second = parse(m));
                else second = parse(n, first);
                s = first.sign !== second.sign;
                var a = first.value, b = second.value;
                var resultSum = [];
                for (var i = 0; i < a.length; i++) {
                    resultSum[i] = [];
                    var j = i;
                    while (j--) {
                        resultSum[i].push(0);
                    }
                }
                var carry = 0;
                for (var i = 0; i < a.length; i++) {
                    var x = a[i];
                    for (var j = 0; j < b.length || carry > 0; j++) {
                        var y = b[j];
                        var product = y ? (x * y) + carry : carry;
                        carry = product > base ? Math.floor(product / base) : 0;
                        product -= carry * base;
                        resultSum[i].push(product);
                    }
                }
                var max = -1;
                for (var i = 0; i < resultSum.length; i++) {
                    var len = resultSum[i].length;
                    if (len > max) max = len;
                }
                var result = [], carry = 0;
                for (var i = 0; i < max || carry > 0; i++) {
                    var sum = carry;
                    for (var j = 0; j < resultSum.length; j++) {
                        sum += resultSum[j][i] || 0;
                    }
                    carry = sum > base ? Math.floor(sum / base) : 0;
                    sum -= carry * base;
                    result.push(sum);
                }
                return bigInt(result, s);
            },
            times: function (n, m) {
                return o.multiply(n, m);
            },
            divmod: function (n, m) {
                var s, first = self, second;
                if (m) (first = parse(n)) && (second = parse(m));
                else second = parse(n, first);
                s = first.sign !== second.sign;
                if (bigInt(first.value, first.sign).equals(0)) return {
                    quotient: bigInt([0], sign.positive),
                    remainder: bigInt([0], sign.positive)
                };
                if (second.equals(0)) throw new Error("Cannot divide by zero");
                var a = first.value, b = second.value;
                var result = [], remainder = [];
                for (var i = a.length - 1; i >= 0; i--) {
                    var n = [a[i]].concat(remainder);
                    var quotient = goesInto(b, n);
                    result.push(quotient.result);
                    remainder = quotient.remainder;
                }
                result.reverse();
                return {
                    quotient: bigInt(result, s),
                    remainder: bigInt(remainder, first.sign)
                };
            },
            divide: function (n, m) {
                return o.divmod(n, m).quotient;
            },
            over: function (n, m) {
                return o.divide(n, m);
            },
            mod: function (n, m) {
                return o.divmod(n, m).remainder;
            },
            pow: function (n, m) {
                var first = self, second;
                if (m) (first = parse(n)) && (second = parse(m));
                else second = parse(n, first);
                var a = first, b = second;
                if (b.lesser(0)) return ZERO;
                if (b.equals(0)) return ONE;
                var result = bigInt(a.value, a.sign);

                if (b.mod(2).equals(0)) {
                    var c = result.pow(b.over(2));
                    return c.times(c);
                } else {
                    return result.times(result.pow(b.minus(1)));
                }
            },
            next: function (m) {
                var first = m || self;
                return o.add(first, 1);
            },
            prev: function (m) {
                var first = m || self;
                return o.subtract(first, 1);
            },
            compare: function (n, m) {
                var first = self, second;
                if (m) (first = parse(n)) && (second = parse(m, first));
                else second = parse(n, first);
                normalize(first, second);
                if (first.value.length === 1 && second.value.length === 1 && first.value[0] === 0 && second.value[0] === 0) return 0;
                if (second.sign !== first.sign) return first.sign === sign.positive ? 1 : -1;
                var multiplier = first.sign === sign.positive ? 1 : -1;
                var a = first.value, b = second.value;
                for (var i = a.length - 1; i >= 0; i--) {
                    if (a[i] > b[i]) return 1 * multiplier;
                    if (b[i] > a[i]) return -1 * multiplier;
                }
                return 0;
            },
            compareAbs: function (n, m) {
                var first = self, second;
                if (m) (first = parse(n)) && (second = parse(m, first));
                else second = parse(n, first);
                first.sign = second.sign = sign.positive;
                return o.compare(first, second);
            },
            equals: function (n, m) {
                return o.compare(n, m) === 0;
            },
            notEquals: function (n, m) {
                return !o.equals(n, m);
            },
            lesser: function (n, m) {
                return o.compare(n, m) < 0;
            },
            greater: function (n, m) {
                return o.compare(n, m) > 0;
            },
            greaterOrEquals: function (n, m) {
                return o.compare(n, m) >= 0;
            },
            lesserOrEquals: function (n, m) {
                return o.compare(n, m) <= 0;
            },
            isPositive: function (m) {
                var first = m || self;
                return first.sign === sign.positive;
            },
            isNegative: function (m) {
                var first = m || self;
                return first.sign === sign.negative;
            },
            isEven: function (m) {
                var first = m || self;
                return first.value[0] % 2 === 0;
            },
            isOdd: function (m) {
                var first = m || self;
                return first.value[0] % 2 === 1;
            },
            toString: function (m) {
                var first = m || self;
                var str = "", len = first.value.length;
                while (len--) {
                    if (first.value[len].toString().length === 8) str += first.value[len];
                    else str += (base.toString() + first.value[len]).slice(-logBase);
                }
                while (str[0] === "0") {
                    str = str.slice(1);
                }
                if (!str.length) str = "0";
                var s = first.sign === sign.positive ? "" : "-";
                return s + str;
            },
            toJSNumber: function (m) {
                return +o.toString(m);
            },
            valueOf: function (m) {
                return o.toJSNumber(m);
            }
        };
        return o;
    };

    var ZERO = bigInt([0], sign.positive);
    var ONE = bigInt([1], sign.positive);
    var MINUS_ONE = bigInt([1], sign.negative);

    var fnReturn = function (a) {
        if (typeof a === "undefined") return ZERO;
        return parse(a);
    };
    fnReturn.zero = ZERO;
    fnReturn.one = ONE;
    fnReturn.minusOne = MINUS_ONE;
    return fnReturn;
})();

if (typeof module !== "undefined") {
    module.exports = bigInt;
}
},{}]},{},[])
;