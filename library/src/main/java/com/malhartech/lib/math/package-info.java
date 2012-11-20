/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 *
 */

/**
 * <b>{@link com.malhartech.lib.math}</b> is a library of arithmetic modules for reuse<p>
 * <br>
 * <br>The modules are<br>
 * <b>{@link com.malhartech.lib.math.BaseNumberKeyValueOperator}</b>: Base class for operators that take in V extends Number. Provides basic methods for value conversion, and cloning of keys and values<br>
 * <b>{@link com.malhartech.lib.math.BaseNumberValueOperator}</b>: Base class for operators that take in K,V extends Number. Provides basic methods for value conversion and key cloning<br>
 * <b>{@link com.malhartech.lib.math.Compare}</b>:Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * passed the test, it is emitted on the output port "compare". The comparison is done by getting double value from the Number. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.math.CompareExcept}</b>: Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * passed the test, it is emitted on the output port "compare". If the tuple fails it is emitted on port "except". The comparison is done by getting double
 * value from the Number. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.math.CompareExceptString}</b>:  * Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * passed the test, it is emitted on the output port "compare". If the tuple fails it is emitted on port "except". The comparison is done parsing a double
 * value from the String. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.math.CompareString}</b>:  Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * passed the test, it is emitted on the output port "compare". The comparison is done by parsing double value from the String.<br>
 * <b>{@link com.malhartech.lib.math.Except}</b>: Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * fails the test, it is emitted on the output port "except". The comparison is done by getting double
 * value from the Number. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.math.ExceptString}</b>:  Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * fails the test, it is emitted on the output port "except". The comparison is done by parsing double value from the String. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.math.Margin}</b>: For every window computes margins of sums of values of a key in two streams. Emits margin of eack key at end of window<br>
 * <b>{@link com.malhartech.lib.math.MarginValue}</b>: For every window computes margins of sums of tuples in two streams. Emits tuple at the end of window<br>
 * <b>{@link com.malhartech.lib.math.Max}</b>: Takes in one stream via input port "data". At end of window sends maximum of all values for each key and emits them on port "max"<br>
 * <b>{@link com.malhartech.lib.math.MaxValue}</b>: Takes in one stream via input port "data". At end of window sends maximum of all incoming tuples on port "max"<br>
 * <b>{@link com.malhartech.lib.math.Min}</b>: Takes in one stream via input port "data". At end of window sends minimum of all values for each key and emits them on port "min"<br>
 * <b>{@link com.malhartech.lib.math.MinValue}</b>: Takes in one stream via input port "data". At end of window sends minimum of all incoming tuples on port "min"<br>
 * <b>{@link com.malhartech.lib.math.Quotient}</b>: Takes in two streams via input ports "numerator" and "denominator". At the end of window computes the quotient for each key and emits the result on port "quotient"<br>
 * <b>{@link com.malhartech.lib.math.QuotientValue}</b>: Takes in two streams via input ports "numerator" and "denominator". At the end of window computes the quotient for all the tuples and emits the result on port "quotient"<br>
 * <b>{@link com.malhartech.lib.math.Range}</b>: Takes in one stream via input port "data". At end of window sends range of all values for each key and emits them on port "range"<br>
 * <b>{@link com.malhartech.lib.math.RangeValue}</b>: Takes in one stream via input port "data". At end of window sends range of all values on port "range"<br>
 * <b>{@link com.malhartech.lib.math.Sum}</b>: Takes in one stream via input port "data". At end of window sums all values for each key and emits them on port sum; emits number of occurrences on port count; and average on port average<br>
 * <b>{@link com.malhartech.lib.math.SumValue}</b>: Takes in one stream via input port "data". At end of window sums all values and emits them on port sum; emits number of occurrences on port count; and average on port average<br>
 * <br>
 */

package com.malhartech.lib.math;
