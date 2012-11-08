/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.logs;

import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * Splits the String tuples into tokens and sub-tokens, and emits HashMap<Key,Value>. Each token is emitted on output port "tokens" as key,val pair if the key exists in the filterby<p>
 * This module is a pass through. <br>
 * <br>
 * Ideal for applications like log processing where only a few keys are to be processed<br>
 * Ports:<br>
 * <b>data</b>: expects String<br>
 * <b>tokens</b>: emits HashMap<String,String><br>
 * <br>
 * Properties:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val pair. Default is "", i.e. tokens are not split, and key is set to token, and val is null<br>
 * <b>filterby</b>: Only emit the keys that are in filterby<br>
 * <br>
 * Compile time checks<br>
 * None<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator processes > 2.5 million tuples/sec. The processing was done with 3 keys per line. The performance is proportional to number of keys
 * and their length. For every tuple processed and average of N tuples are emitted, where N is the average number of keys per tuple<br>
 * <br>
 *
 * @author amol<br>
 * <br>
 *
 */
public class FilteredLineTokenizerKeyVal extends LineTokenizerKeyVal
{
  HashMap<String, Object> subTokenFilters = new HashMap<String, Object>(4);

  public void setSubTokenFilters(ArrayList<String> list)
  {
    for (String s: list) {
      subTokenFilters.put(s, null);
    }
  }

  @Override
  public boolean validSubTokenKey(String subtok)
  {
    return super.validToken(subtok) && subTokenFilters.containsKey(subtok);
  }
}
