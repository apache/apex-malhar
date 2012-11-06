/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in one stream via input port "data". The tuples are String objects and are split into tkns. An ArrayList of all tkns are emitted on output port "tkns"<p>
 * This module is a pass through<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: expects String<br>
 * <b>tokens</b>: emits ArrayList<String><br>
 * <b>splittokens</b>: emits ArrayList<HashMap<String,ArrayList<String>>><br>
 * <br>
 * Properties:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val1,val2,.... If not specified the value is set to null. Default is "", i.e. tokens are not split<br>
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
 * @author amol
 */
public class LineToTokenArrayList extends BaseLineSplitter
{
  @OutputPortFieldAnnotation(name = "tokens")
  public final transient DefaultOutputPort<ArrayList<String>> tokens = new DefaultOutputPort<ArrayList<String>>(this);
  @OutputPortFieldAnnotation(name = "splittokens")
  public final transient DefaultOutputPort<ArrayList<HashMap<String, ArrayList<String>>>> splittokens = new DefaultOutputPort<ArrayList<HashMap<String, ArrayList<String>>>>(this);

  private ArrayList<String> tokentuple = null;
  private ArrayList<HashMap<String, ArrayList<String>>> stokentuple = null;
  private HashMap<String, ArrayList<String>> smap = null;
  private ArrayList<String> vals = null;

  @Override
  public void beginProcessTokens()
  {
    tokentuple = new ArrayList<String>();
    stokentuple = new ArrayList<HashMap<String, ArrayList<String>>>();
  }

  @Override
  public void processToken(String tok)
  {
    if (tokens.isConnected()) {
      tokentuple.add(tok);
    }
    if (splittokens.isConnected() && !splitTokenBy.isEmpty()) {
      super.processToken(tok);
    }
  }

  @Override
  public void beginProcessSubTokens()
  {
    smap = new HashMap<String, ArrayList<String>>(1);
    vals = new ArrayList<String>(4);
  }

  @Override
  public void endProcessSubTokens()
  {
    stokentuple.add(smap);
    smap = null;
    vals = null;
  }

  @Override
  public void processSubToken(String subtok)
  {
    if (smap.isEmpty()) {
      smap.put(subtok, vals);
    }
    else {
      vals.add(subtok);
    }
  }

  @Override
  public void endProcessTokens()
  {
    if (tokens.isConnected()) {
      tokens.emit(tokentuple);
      tokentuple = null;
    }

    if (splittokens.isConnected()) {
      splittokens.emit(stokentuple);
      stokentuple = null;
      smap = null;
      vals = null;
    }
  }
}
