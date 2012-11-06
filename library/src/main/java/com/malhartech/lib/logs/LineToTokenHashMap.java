/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * Takes in one stream via input port "data". The tuples are String objects and are split into tokens. A HashMap of all tokens are emitted on output port "tokens".<p>
 * This module is a pass through<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects String<br>
 * <b>tokens</b>: Output port, emits HashMap<String, ArrayList<String>><br>
 * <br>
 * Properties:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val1,val2,.... Default is "", i.e. tokens are not split, and key=token, val=""<br>
 * <br>
 * Compile time checks<br>
 * None<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator processes > 2 million tuples/sec. The benchmarking was done with 3 keys per line. The performance is proportional to number of keys
 * and their length. For every tuple processed and average of N tuples are emitted, where N is the average number of keys per tuple<br>
 * <br>
 * @author amol<br>
 * <br>
 */
public class LineToTokenHashMap extends BaseLineTokenizer
{
  @OutputPortFieldAnnotation(name = "tokens")
  public final transient DefaultOutputPort<HashMap<String, ArrayList<String>>> tokens = new DefaultOutputPort<HashMap<String, ArrayList<String>>>(this);

  private HashMap<String, ArrayList<String>> otuple = null;
  private ArrayList<String> vals = null;
  private String tok = "";

  @Override
  public void beginProcessTokens()
  {
    otuple = new HashMap<String, ArrayList<String>>();
  }

  @Override
  public void beginProcessSubTokens()
  {
    vals = null;
    tok = "";
  }

  @Override
  public void processSubToken(String subtok)
  {
    if (vals == null) {
      tok = subtok;
      vals = new ArrayList<String>();
    }
    else {
      vals.add(subtok);
    }
  }

  @Override
  public void endProcessSubTokens()
  {
    otuple.put(tok, vals);
    tok = "";
    vals = null;
  }

  @Override
  public void endProcessTokens()
  {
    if (tokens.isConnected()) {
      tokens.emit(otuple);
      otuple = null;
    }
  }
}
