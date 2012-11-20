/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.lib.util.BaseLineTokenizer;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * Splits String objects into tokens, and emits as HashMap.<p>
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

  /**
   * sets up output tuple
   */
  @Override
  public void beginProcessTokens()
  {
    otuple = new HashMap<String, ArrayList<String>>();
  }


  /**
   * clears data for subtokens
   */
  @Override
  public void beginProcessSubTokens()
  {
    vals = null;
    tok = "";
  }

  /**
   * first token is key, the rest are added to ArrayList
   * @param subtok
   */
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


  /**
   * Adds key,Arraylist pair to output tuple
   */
  @Override
  public void endProcessSubTokens()
  {
    otuple.put(tok, vals);
    tok = "";
    vals = null;
  }

  /**
   * emits output tuple
   */
  @Override
  public void endProcessTokens()
  {
    if (tokens.isConnected()) {
      tokens.emit(otuple);
      otuple = null;
    }
  }
}
