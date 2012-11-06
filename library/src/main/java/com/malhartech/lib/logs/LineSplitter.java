/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;

/**
 *
 * Takes in one stream via input port "data". The tuples are String objects and are split into String tokens. Each token is emitted on output port "tokens"<p>
 * This module is a pass through<br>
 * <br>
 * Ideal for applications like word count
 * Ports:<br>
 * <b>data</b>: expects String<br>
 * <b>tokens</b>: emits String<br>
 * <br>
 * Properties:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <br>
 * Compile time checks<br>
 * None<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator processes > 9 million tuples/sec. The processing was done with 3 keys per line. The performance is proportional to number of keys
 * and their length. For every tuple processed and average of N tuples are emitted, where N is the average number of keys per tuple<br>
 * <br>
 * @author amol<br>
 * <br>
 *
 */
public class LineSplitter extends BaseLineTokenizer
{
  @OutputPortFieldAnnotation(name = "tokens")
  public final transient DefaultOutputPort<String> tokens = new DefaultOutputPort<String>(this);

  @Override
    public void processToken(String tok)
  {
    if (!tok.isEmpty()) {
      tokens.emit(tok);
    }
  }
}
