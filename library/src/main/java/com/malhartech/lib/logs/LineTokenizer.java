/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseLineTokenizer;

/**
 *
 * Splits lines into tokens and emits Strings<p>
 * This module is a pass through. Ideal for applications like word count, or log processing<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects String<br>
 * <b>tokens</b>: emits String<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for LineTokenizer operator">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 9 Million tuples/s (for N=3)</b></td><td>For every in-bound tuple N tuples are emitted, where N is the average number of keys per tuple</td>
 * <td>In-bound rate and the number of keys in the String are the main determinant of performance</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (splitby=",")</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for LineTokenizer operator">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(String)</th><th><i>tokens</i>(String)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>"2,a,b=5,33,f"</td><td>"2" ; "a" ; "b=5" ; "33" ; "f"</td></tr>
 * <tr><td>Data (process())</td><td>""</td><td></td></tr>
 * <tr><td>Data (process())</td><td>"a,,b,d"</td><td>"a" ; "b" ; "d"</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class LineTokenizer extends BaseLineTokenizer
{
  @OutputPortFieldAnnotation(name = "tokens")
  public final transient DefaultOutputPort<String> tokens = new DefaultOutputPort<String>(this);

  /**
   * emits tokens on port "tokens" if tok is not empty
   * @param tok
   */
  @Override
    public void processToken(String tok)
  {
    if (!tok.isEmpty()) {
      tokens.emit(tok);
    }
  }
}

