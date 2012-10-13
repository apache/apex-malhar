/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 *
 */

/**
 * <b>com.malhartech.lib.math</b> is a library of arithmetic modules for reuse<p>
 * <br>
 * <br>The modules are<br>
 * <b>{@link com.malhartech.lib.math.Compare}</b>:Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * passed the test, it is emitted on the output port "compare". The comparison is done by getting double value from the Number. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.math.CompareExcept}</b>: Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * passed the test, it is emitted on the output port "compare". If the tuple fails it is emitted on port "except". The comparison is done by getting double
 * value from the Number. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.math.Except}</b>: Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * fails the test, it is emitted on the output port "except". The comparison is done by getting double
 * value from the Number. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.math.Max}</b>: Takes in one stream via input port "data". At end of window sends maximum of all values for each key and emits them on port "max"<br>
 * <b>{@link com.malhartech.lib.math.Min}</b>: Takes in one stream via input port "data". At end of window sends minimum of all values for each key and emits them on port "min"<br>
 * <b>{@link com.malhartech.lib.math.Margin}</b>: For every window computes margins of sums of values of a key in two streams<br>
 * <b>{@link com.malhartech.lib.math.Quotient}</b>: For every window computes quotient of sum of values of a key in two streams<br>
 * <b>{@link com.malhartech.lib.math.Range}</b>: Takes in one stream via input port "data". At end of window sends range of all values for each key and emits them on port "range"<br>
 * <b>{@link com.malhartech.lib.math.Sum}</b>: For every window adds all the values of a key in a stream. For string values users can choose to appends with a delimiter<br>
 * <b>{@link com.malhartech.lib.math.UniqueCounter}</b>: Counts payloads and send unique count for each on end of window<br>
 * <br>
 */

package com.malhartech.lib.math;
