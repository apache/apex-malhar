/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.logs;

import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * Splits the String tuples into tokens and sub-tokens, and emits HashMap. Each token is emitted on output port "tokens" as key,val pair if the key exists in the filterby<p>
 * This module is a pass through. Ideal for applications like log processing where only a few keys are to be processed<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects String<br>
 * <b>tokens</b>: emits HashMap&lt;String,String&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val pair. Default is "", i.e. tokens are not split, and key is set to token, and val is null<br>
 * <b>filterby</b>: Only emit the keys (comma separated) that are in filterby<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for FilteredLineTokenizerKeyVal operator">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 2.5 Million tuples/s (for N=3)</b></td><td>For every in-bound tuple N tuples are emitted, where N is the average number of keys per tuple</td>
 * <td>In-bound rate and the number of keys in the String are the main determinant of performance</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (splitby=",", splittokenby="=", filterby="a,c")</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for FilteredLineTokenizerKeyVal operator">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(String)</th><th><i>tokens</i>(HashMap&lt;String,String&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>"a=2,a=5,b=5,c=33,f"</td><td>{a=2,a=5,c=33}</td></tr>
 * <tr><td>Data (process())</td><td>""</td><td></td></tr>
 * <tr><td>Data (process())</td><td>"a=d,,b=66"</td><td>{a=d}</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class FilteredLineTokenizerKeyVal extends LineTokenizerKeyVal
{
  HashMap<String, Object> filterBy = new HashMap<String, Object>(4);

  /**
   * setter function for filterBy
   * @param list list of keys for subtoken filters
   */
  public void setFilterBy(String [] list)
  {
    if (list != null) {
      for (String s: list) {
        filterBy.put(s, null);
      }
    }
  }

  /**
   * If the key is in the filter, returns true
   * @param subtok
   * @return true if super.validToken (!isEmpty()) and filter has they token
   */
  @Override
  public boolean validSubTokenKey(String subtok)
  {
    return super.validToken(subtok) && filterBy.containsKey(subtok);
  }
}
