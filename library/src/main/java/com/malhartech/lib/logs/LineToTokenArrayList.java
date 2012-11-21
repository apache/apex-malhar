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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Splits String objects into tokens, and emits as ArrayList. An ArrayList of all tkns are emitted on output port "tokens". An ArrayList
 * of all subtokens are emitted on port splittokens<p>
 * This module is a pass through. Ideal for applications like log processing<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects String<br>
 * <b>tokens</b>: emits ArrayList&lt;String&gt;<br>
 * <b>splittokens</b>: emits ArrayList&lt;HashMap&lt;String,ArrayList&lt;String&gt;&gt;&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val1,val2,.... If not specified the value is set to null. Default is "", i.e. tokens are not split<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for LineToTokenArrayList operator">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 2 Million tuples/s (for N=3)</b></td><td>For every in-bound tuple N key,val pairs are emitted per port, where N is the average number of keys per String</td>
 * <td>In-bound rate and the number of keys in the String are the main determinant of performance</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (splitby=",", splittokenby="=")</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for LineToTokenArrayList operator">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th colspan=2>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(String)</th><th><i>tokens</i>(ArrayList&lt;String&gt;)</th><th><i>splittokens</i>(ArrayList&lt;HashMap&lt;String,ArrayList&lt;String&gt;&gt;(1)&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>"a=2,a=5,b=5,c=33=5,f"</td><td>["a=2","a=5","b=5","c=33=5","f="]</td><td>[{a=[2]},{a=[5]},{b=[5]},{c=[33,5]},{f=[]}]</td></tr>
 * <tr><td>Data (process())</td><td>""</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>"a=d,,b=66"</td><td>["a=d","b=66"]</td><td>[{a=[d]},{b=[66]}]</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class LineToTokenArrayList extends BaseLineTokenizer
{
  @OutputPortFieldAnnotation(name = "tokens")
  public final transient DefaultOutputPort<ArrayList<String>> tokens = new DefaultOutputPort<ArrayList<String>>(this);
  @OutputPortFieldAnnotation(name = "splittokens")
  public final transient DefaultOutputPort<ArrayList<HashMap<String, ArrayList<String>>>> splittokens = new DefaultOutputPort<ArrayList<HashMap<String, ArrayList<String>>>>(this);

  private ArrayList<String> tokentuple = null;
  private ArrayList<HashMap<String, ArrayList<String>>> stokentuple = null;
  private HashMap<String, ArrayList<String>> smap = null;
  private ArrayList<String> vals = null;

  /**
   * sets up output tuples
   */
  @Override
  public void beginProcessTokens()
  {
    tokentuple = new ArrayList<String>();
    stokentuple = new ArrayList<HashMap<String, ArrayList<String>>>();
  }

  /**
   * adds tok to output token tuple
   * if splittoken is set starts subtoken processing
   * @param tok
   */
  @Override
  public void processToken(String tok)
  {
    if (tokens.isConnected()) {
      tokentuple.add(tok);
    }
    if (splittokens.isConnected() && hasSplitTokenBy()) {
      super.processToken(tok);
    }
  }

  /**
   * sets up data for sub token processing
   */
  @Override
  public void beginProcessSubTokens()
  {
    smap = new HashMap<String, ArrayList<String>>(1);
    vals = new ArrayList<String>(4);
  }

  /**
   * Added data to subtoken tuple
   */
  @Override
  public void endProcessSubTokens()
  {
    if (!smap.isEmpty()) {
      stokentuple.add(smap);
    }
    smap = null;
    vals = null;
  }

  /**
   * Addd first subtoken to key, and rest to value ArrayList
   * @param subtok
   */
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

  /**
   * emits token tuple and subtoken tuple if respective ports are connected
   */
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
