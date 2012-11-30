/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 *
 */

/**
 * <b>{@link com.malhartech.lib.math}</b> is a library of arithmetic modules for reuse<p>
 * <br>
 * <br>The modules are<br>
 * <b>{@link com.malhartech.lib.math.Change}</b>: Emits the change in the value of the key in stream on port data (as compared to a base value set via port base) for every tuple<br>
 * <b>{@link com.malhartech.lib.math.ChangeAlert}</b>: Emits the change in the value of the key in stream on port data (as compared to a base value set via port base) for every tuple<br>
 * <b>{@link com.malhartech.lib.math.Compare}</b>: A compare operation is done on tuple subclassed from Number based on the property "key", "value", and "cmp",
 * and matching tuples are emitted. If the tuple passed the test, it is emitted on the output port "compare". The comparison is done by getting double
 * value from the Number. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.math.CompareExcept}</b>: A compare opertion is done based on the property "key", "value", and "compare". If the tuple
 * passed the test, it is emitted on the output port "compare". If the tuple fails it is emitted on port "except". The comparison is done by getting double
 * value from the Number. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.math.CompareExceptString}</b>: A compare operation is done on String tuple based on the property "key", "value", and "cmp" both matching and non matching tuples on emitted on respective ports. If the tuple
 * passed the test, it is emitted on the output port "compare". If the tuple fails it is emitted on port "except". The comparison is done parsing a double
 * value from the String. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.math.CompareString}</b>: A compare operation is done on String tuples based on the property "key", "value", and "cmp" and all matching tuples are emitted. If the tuple
 * passed the test, it is emitted on the output port "compare". The comparison is done by parsing double value from the String<br>
 * <b>{@link com.malhartech.lib.math.Except}</b>: A compare operation is done on tuple sub-classed from Number based on the property "key", "value", and "cmp", and not matched tuples are emitted.
 * The comparison is done by getting double value from the Number. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.math.ExceptString}</b>: A compare operation is done on tuple sub-classed from Number based on the property "key", "value", and "cmp", and not matched tuples are emitted.
 * The comparison is done by parsing double value from the String. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.math.Margin}</b>: Adds all values for each key in "numerator" and "denominator", and at the end of window emits the margin for each key
 * (1 - numerator/denominator)<br>
 * <b>{@link com.malhartech.lib.math.MarginValue}</b>:Adds all values for each key in "numerator" and "denominator", and at the end of window emits the margin as
 * (1 - numerator/denominator)<br>
 * <b>{@link com.malhartech.lib.math.Max}</b>: Emits at end of window maximum of all values sub-classed from Number for each key<br>
 * <b>{@link com.malhartech.lib.math.MaxValue}</b>: Emits at end of window maximum of all values sub-classed from Number in the incoming stream<br>
 * <b>{@link com.malhartech.lib.math.Min}</b>: Emits at end of window minimum of all values sub-classed from Number for each key<br>
 * <b>{@link com.malhartech.lib.math.MinValue}</b>: Emits at end of window minimum of all values sub-classed from Number in the incoming stream<br>
 * <b>{@link com.malhartech.lib.math.Quotient}</b>: Add all the values for each key on "numerator" and "denominator" and emits quotient at end of window for all keys in the denominator<br>
 * <b>{@link com.malhartech.lib.math.QuotientValue}</b>: Adds all the values on "numerator" and "denominator" and emits quotient at end of window<br>
 * <b>{@link com.malhartech.lib.math.Range}</b>: Emits the range for each key at the end of window<br>
 * <b>{@link com.malhartech.lib.math.RangeValue}</b>: Emits the range of values at the end of window<br>
 * <b>{@link com.malhartech.lib.math.Sum}</b>: Emits the sum, average, and count of values for each key at the end of window<br>
 * <b>{@link com.malhartech.lib.math.SumValue}</b>: Emits the sum, average, and count of values at the end of window<br>
 * <br>
 */

package com.malhartech.lib.math;
