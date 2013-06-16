/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.math;

/**
 *
 * Emits the result of square of the input tuple (Number).<br>
 * Emits the result as Long on port longResult, as Integer on port integerResult,
 * as Double on port doubleResult, and as Float on port floatResult. This is a pass through operator<p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>input</b>: expects Number<br>
 * <b>longResult</b>: emits Long<br>
 * <b>integerResult</b>: emits Integer<br>
 * <b>doubleResult</b>: emits Double<br>
 * <b>floatResult</b>: emits Float<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for SquareCalculus">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>25 million tuples/s</b></td><td>four tuples per one incoming tuple</td><td>Performance is i/o bound and directly
 * dependant on incoming tuple rate</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (multiplier = 2.0)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for SquareCalculus">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>input</i>::process)</th><th colspan=4>Out-bound (emit)</th></tr>
 * <tr><th><i>input</i></th><th><i>longResult</i></th><th><i>integerResult</i></th><th><i>doubleResult</i></th><th><i>floatResult</i></th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>2</td><td>4</td><td>4</td><td>4.0</td><td>4.0</td></tr>
 * <tr><td>Data (process())</td><td>-12</td><td>144</td><td>144</td><td>144.0</td><td>144.0</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class SquareCalculus extends SingleVariableAbstractCalculus
{
  @Override
  public double function(double dval)
  {
    return dval * dval;
  }

  @Override
  public long function(long lval)
  {
    return lval * lval;
  }

}
