/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.logs;


import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * Takes in one stream via input port "data". The tuples are String objects and are split into tokens. An ArrayList of all tokens that pass the filter are emitted on output port "tokens"<p>
 *  This module is a pass through<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects String<br>
 * <b>tokens</b>: Output port, emits ArrayList<Object><br>
 * <br>
 * Properties:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val pair. If not specified the value is set to null. Default is ",", i.e. tokens are split<br>
 * <b>filterby</b>: Only emit the keys that are in filterby<br>
 * <br>
 * Compile time checks<br>
 * Property "splittokenby" cannot be empty<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <br>
* <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator processes > 2 million tuples/sec. The benchmarking was done with 3 keys per line. The performance is proportional to number of keys
 * and their length. For every tuple processed and average of N tuples are emitted, where N is the average number of keys per tuple<br>
 * <br>
 * @author amol
 */

public class FilteredLineToTokenArrayList extends LineToTokenArrayList
{
  HashMap<String, Object> subTokenFilters = new HashMap<String, Object>();

  public void setSubTokenFilters(ArrayList<String> list) {
    for (String s : list) {
      subTokenFilters.put(s, null);
    }
  }

  @Override
  public boolean validSubTokenKey(String subtok) {
    return super.validToken(subtok) && subTokenFilters.containsKey(subtok);
  }
}
