/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.GenericNode;
import com.malhartech.api.OperatorConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in one stream via input port "data". The tuples are String objects and are split into tokens. A HashMap of all filtered tokens are emitted on output port "tokens"<p>
 *  This module is a pass through<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects String<br>
 * <b>tokens</b>: Output port, emits HashMap<String, Object><br>
 * <br>
 * Properties:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val pair. If not specified the value is set to null. Default is ",", i.e. tokens are split<br>
 * <b>filterby</b>: Only emit the keys that are in filterby<br>
 * <br>
 * Compile time checks<br>
 * Property "splittokenby" cannot be empty<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * TBD<br>
 *
 * @author amol
 */


public class FilteredLineToTokenHashMap extends LineToTokenHashMap
{
  private static Logger LOG = LoggerFactory.getLogger(FilteredLineToTokenHashMap.class);

  HashMap<String, Object> filters = null;

  /**
   * Comma separated list of keys to pass through
   */
  public static final String KEY_FILTERBY = "filterby";

  @Override
  public boolean addToken(String t) {
    return super.addToken(t) && filters.containsKey(t);
  }

  @Override
  public boolean myValidation(OperatorConfiguration config)
  {
    boolean ret = super.myValidation(config);
    if (!dosplittoken) {
      ret = false;
      throw new IllegalArgumentException(String.format("Property \"%s\" has to be specified", KEY_SPLITTOKENBY));
    }
    return ret;
  }
   /**
   *
   * @param config
   */
  @Override
  public void setup(OperatorConfiguration config)
  {
    super.setup(config);

    String[] fstr = config.getTrimmedStrings(KEY_FILTERBY);
    filters = new HashMap<String, Object>();
    for (String f : fstr) {
      filters.put(f, null);
    }
    LOG.debug(String.format("Set up: filter by \"%s\"", fstr));
  }
}
