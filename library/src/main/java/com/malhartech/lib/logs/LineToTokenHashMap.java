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
 * Takes in one stream via input port "data". The tuples are String objects and are split into tokens. A HashMap of all tokens are emitted on output port "tokens"<p>
 *  This module is a pass through<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects String<br>
 * <b>tokens</b>: Output port, emits HashMap<String, Object><br>
 * <br>
 * Properties:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val pair. If not specified the value is set to null. Default is "", i.e. tokens are not split<br>
 * <br>
 * Compile time checks<br>
 * None<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * TBD<br>
 *
 * @author amol
 */


@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = LineToTokenHashMap.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = LineToTokenHashMap.OPORT_TOKENS, type = PortAnnotation.PortType.OUTPUT)
})
public class LineToTokenHashMap extends AbstractModule
{
  public static final String IPORT_DATA = "data";
  public static final String OPORT_TOKENS = "tokens";
  private static Logger LOG = LoggerFactory.getLogger(LineToTokenHashMap.class);

  String splitby_default = new String(";\t ");
  String splittokenby_default = new String();
  String splitby = null;
  String splittokenby = null;
  boolean dosplittoken = false; // !splittokenby_default.isEmpty();

  /**
   * Tokens are split by this string
   *
   */
  public static final String KEY_SPLITBY = "splitby";

  /**
   * The value to compare with
   *
   */
  public static final String KEY_SPLITTOKENBY = "splittokenby";


  public boolean addToken(String t) {
    return !t.isEmpty();
  }


  /**
   * Process each tuple
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    String line = (String) payload;
    if (!line.isEmpty()) {
      // emit error token?
      return;
    }
    String[] tokens = line.split(splitby);
    HashMap<String, Object> tuple = new HashMap<String, Object>();
    for (String t : tokens) {
      if (addToken(t)) {
        if (dosplittoken) {
          String[] vals = t.split(splittokenby);
          if (vals.length != 0) {
            String key = vals[0];
            if (vals.length == 1) {
              tuple.put(t, null);
            }
            else if (vals.length == 2) {
             tuple.put(vals[0], vals[1]);
            }
            else { // For now do ArrayList
              ArrayList list = new ArrayList(vals.length);
              for (int i = 1; i < vals.length; i++) {
                list.add(vals[i]);
              }
              tuple.put(vals[0], list);
            }
          }
        }
        else {
          tuple.put(t, null);
        }
      }
      // should emit error in else clause?
    }
    if (!tuple.isEmpty()) {
      emit(tuple);
    }
    // should emit error if tuple is empty?
  }


  public boolean myValidation(ModuleConfiguration config)
  {
    boolean ret = true;
    return ret;
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
    splittokenby = config.get(KEY_SPLITTOKENBY, splittokenby_default);
    dosplittoken = !splittokenby.isEmpty();

    LOG.debug(String.format("Set up: split by is \"%s\", splittokenby is \"%s\"", splitby, splittokenby));
  }
}
