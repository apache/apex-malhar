/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.logs;


import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * Takes in one stream via input port "data". The tuples are String objects and are split into tokens. A HashMap of all filtered tokens are emitted on output port "tokens"<p>
 *  This module is a pass through<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects String<br>
 * <b>tokens</b>: Output port, emits HashMap<String, Object><br>
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
 * TBD<br>
 *
 * @author amol
 */


public class FilteredLineToTokenHashMap extends LineToTokenHashMap
{
  HashMap<String, Object> filters = new HashMap<String, Object>(4);

  public void setFilters(ArrayList<String> list) {
    for (String s : list) {
      filters.put(s, null);
    }
  }

  @Override
  public boolean addToken(String t) {
    return super.addToken(t) && filters.containsKey(t);
  }
}
