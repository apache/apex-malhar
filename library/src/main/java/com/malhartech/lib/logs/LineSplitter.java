/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in one stream via input port "data". The tuples are String objects and are split into String tokens. Each token is emitted on output port "tokens"<p>
 *  This module is a pass through<br>
 * <br>
 * Ideal for applications like word count
 * Ports:<br>
 * <b>data</b>: Input port, expects String<br>
 * <b>tokens</b>: Output port, emits Strings<Object><br>
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
 * @author amol
 */


@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = LineToTokenArrayList.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = LineToTokenArrayList.OPORT_TOKENS, type = PortAnnotation.PortType.OUTPUT)
})
public class LineSplitter extends AbstractModule
{
  public static final String IPORT_DATA = "data";
  public static final String OPORT_TOKENS = "tokens";
  private static Logger LOG = LoggerFactory.getLogger(LineSplitter.class);


  String splitby_default = ";\t ";
  String splitby = null;

   /**
   * Tokens are split by this string
   *
   */
  public static final String KEY_SPLITBY = "splitby";


  public boolean myValidation(ModuleConfiguration config)
  {
    return true;
  }
   /**
   *
   * @param config
   */
  @Override
  public void setup(ModuleConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new FailedOperationException("validation failed");
    }
    splitby = config.get(KEY_SPLITBY, splitby_default);
  }


  @Override
  public void process(Object payload)
  {
    String line = (String) payload;
    if (!line.isEmpty()) {
      // emit error token?
      return;
    }
    String[] tokens = line.split(splitby);
    for (String t : tokens) {
      if (!t.isEmpty()) {
        emit(t);
      }
    }
  }
}
