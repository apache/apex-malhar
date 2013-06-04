/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseLineTokenizer;
import java.util.HashMap;

/**
 *
 * Splits lines into tokens, and tokens into sub-tokens and emits key,val pairs in a HashMap. Useful to convert String (log lines) into a POJO (HashMap)<p>
 * This module is a pass through<br>
 * <br>
 * Ideal for applications like log processing<br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects String<br>
 * <b>tokens</b>: emits HashMap&lt;String,String&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val pair. Default is "", i.e. tokens are not split, and key is set to token, and val is null<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for LineTokenizerKeyVal operator">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 2.5 Million tuples/s (for N=3)</b></td><td>For every in-bound tuple N tuples are emitted, where N is the average number of keys per tuple</td>
 * <td>In-bound rate and the number of keys in the String are the main determinant of performance</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (splitby=",", splittokenby="=")</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for LineTokenizerKeyVal operator">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(String)</th><th><i>tokens</i>(HashMap&lt;String,String&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>"a=2,a=5,b=5,c=33,f"</td><td>{a=2,a=5,b=5,c=33,f=}</td></tr>
 * <tr><td>Data (process())</td><td>""</td><td></td></tr>
 * <tr><td>Data (process())</td><td>"a=d,,b=66"</td><td>{a=d,b=66}</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class LineTokenizerKeyVal extends BaseLineTokenizer
{
  @OutputPortFieldAnnotation(name = "tokens")
  public final transient DefaultOutputPort<HashMap<String, String>> tokens = new DefaultOutputPort<HashMap<String, String>>(this);

  private transient HashMap<String, String> map = null;
  private transient String skey = "";
  private transient String sval = "";

  /**
   * sets up the cache
   */
  @Override
  public void beginProcessTokens()
  {
    map = new HashMap<String, String>();
  }

  /**
   * emits tokens on port "tokens", and clears the cache
   */
  @Override
  public void endProcessTokens()
  {
    if (map != null) {
      tokens.emit(map);
      map = null;
    }
  }

  /**
   * clears subtoken key,val pair
   */
  @Override
  public void beginProcessSubTokens()
  {
    skey = "";
    sval = "";
  }

  /**
   * inserts subtoken key,val pair in subtoken hash. If there are multiple keys with the same value
   * override this call and append values
   */
  @Override
  public void endProcessSubTokens()
  {
    if (!skey.isEmpty()) {
      map.put(skey, sval);
      skey = "";
      sval = "";
    }
  }

  /**
   * first subtoken is the key, the next is the val.
   * No error is flagged for third token as yet.
   * @param subtok
   */
  @Override
  public void processSubToken(String subtok)
  {
    if (skey.isEmpty()) {
      skey = subtok;
    }
    else if (sval.isEmpty()) {
      sval = subtok;
    }
    else {
      // emit error(?)
    }
  }
}
