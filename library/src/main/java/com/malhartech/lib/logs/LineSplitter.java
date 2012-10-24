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
 * TBD<br>
 * <br>
 *
 * @author amol
 */
public class LineSplitter extends BaseOperator
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<String> data = new DefaultInputPort<String>(this)
  {
    @Override
    public void process(String tuple)
    {
      if (!tuple.isEmpty()) {
        // emit error token?
        return;
      }
      String[] tkns = tuple.split(splitby);
      for (String t : tkns) {
        if (!t.isEmpty()) {
          tokens.emit(t);
        }
      }
    }
  };
  @OutputPortFieldAnnotation(name = "tokens")
  public final transient DefaultOutputPort<String> tokens = new DefaultOutputPort<String>(this);
  String splitby_default = ";\t ";
  String splitby = null;

  void setSplitby(String str)
  {
    splitby = str;
  }
}
