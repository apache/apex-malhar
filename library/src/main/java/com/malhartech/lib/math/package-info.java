/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 *
 */

/**
 * <b>com.malhartech.lib.math</b> is a library of arithmetic modules for reuse<p>
 * <br>
 * <br>The modules are<br>
 * <b>{@link com.malhartech.lib.math.ArithmeticMargin}</b>: For every window computes margins of sums of values of a key in two streams<br>
 * <b>{@link com.malhartech.lib.math.ArithmeticQuotient}</b>: For every window computes quotient of sum of values of a key in two streams<br>
 * <b>{@link com.malhartech.lib.math.ArithmeticRange}</b>: Takes in one stream via input port "data". At end of window sends range of all values for each key and emits them on port "range"<br>
 * <b>{@link com.malhartech.lib.math.ArithmeticSum}</b>: For every window adds all the values of a key in a stream. For string values users can choose to appends with a delimiter<br>
 * <b>{@link com.malhartech.lib.math.UniqueCounter}</b>: Counts payloads and send unique count for each on end of window<br>
 * <br>
 */

package com.malhartech.lib.math;
