/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseLineTokenizer;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * Splits String objects into tokens, and emits as HashMap.<p>
 * This module is a pass through<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Input port, expects String<br>
 * <b>tokens</b>: Output port, emits HashMap&lt;String, ArrayList&lt;String&gt;&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val1,val2,.... Default is "", i.e. tokens are not split, and key=token, val=""<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for LineToTokenHashMap operator">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 2 Million tuples/s (for N=3)</b></td><td>For every in-bound tuple N key,val pairs are emitted, where N is the average number of keys per String</td>
 * <td>In-bound rate and the number of keys in the String are the main determinant of performance</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (splitby=",", splittokenby="=")</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for LineToTokenHashMap operator">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(String)</th><th><i>tokens</i>(ArrayList&lt;String&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>"a=2,a=5,b=5,c=33=5,f"</td><td>{a=[2],a=[5],b=[5],c=[33,5],f=[]}</td></tr>
 * <tr><td>Data (process())</td><td>""</td><td></td></tr>
 * <tr><td>Data (process())</td><td>"a=d,,b=66"</td><td>{a=[d],b=[66]}</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
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
