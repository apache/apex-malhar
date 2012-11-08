/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import java.util.HashMap;

/**
 *
 * Splits lines into tokens, and tokens into sub-tokens and emits HashMap<Key,Val><p>
 * This module is a pass through. <br>
 * <br>
 * Ideal for applications like log processing<br>
 * Ports:<br>
 * <b>data</b>: expects String<br>
 * <b>tokens</b>: emits HashMap<String,String><br>
 * <br>
 * Properties:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val pair. Default is "", i.e. tokens are not split, and key is set to token, and val is null<br>
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
public class LineTokenizerKeyVal extends BaseLineTokenizer
{
  @OutputPortFieldAnnotation(name = "tokens")
  public final transient DefaultOutputPort<HashMap<String, String>> tokens = new DefaultOutputPort<HashMap<String, String>>(this);

  private HashMap<String, String> map = null;
  private String skey = "";
  private String sval = "";

  @Override
  public void beginProcessTokens()
  {
    map = new HashMap<String, String>();
  }

  @Override
  public void endProcessTokens()
  {
    if (map != null) {
      tokens.emit(map);
      map = null;
    }
  }

  @Override
  public void beginProcessSubTokens()
  {
    skey = "";
    sval = "";
  }

  @Override
  public void endProcessSubTokens()
  {
    if (!skey.isEmpty()) {
      map.put(skey, sval);
    }
  }

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
