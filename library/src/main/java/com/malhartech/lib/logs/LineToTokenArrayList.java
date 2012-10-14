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
 * Takes in one stream via input port "data". The tuples are String objects and are split into tokens. An ArrayList of all tokens are emitted on output port "tokens"<p>
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
 * Integer: ?? million tuples/s<br>
 * Double: ?? million tuples/s<br>
 * Long: ?? million tuples/s<br>
 * Short: ?? million tuples/s<br>
 * Float: ?? million tupels/s<br>
 *
 * @author amol
 */


@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = LineToTokenArrayList.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = LineToTokenArrayList.OPORT_TOKENS, type = PortAnnotation.PortType.OUTPUT)
})
public class LineToTokenArrayList extends AbstractModule
{
  public static final String IPORT_DATA = "data";
  public static final String OPORT_TOKENS = "tokens";
  private static Logger LOG = LoggerFactory.getLogger(LineToTokenArrayList.class);

  String splitby_default = new String(";\t ");
  String splittokenby_default = new String();
  String splitby = null;
  String splittokenby = null;
  boolean dosplittoken = false; // !splittokenby_default.isEmpty();
   /**
   * The key to compare on
   *
   */
  public static final String KEY_SPLITBY = "splitby";

  /**
   * The value to compare with
   *
   */
  public static final String KEY_SPLITTOKENBY = "splittokenby";

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
    ArrayList<Object> tuple = new ArrayList<Object>();
    for (String t : tokens) {
      if (!t.isEmpty()) {
        if (dosplittoken) {
          String[] vals = t.split(splittokenby);
          if (vals.length != 0) {
            String key = vals[0];
            if (vals.length == 1) {
              tuple.add(t);
            }
            else if (vals.length == 2) {
              HashMap<String, Object> item = new HashMap<String, Object>(1);
              item.put(vals[0], vals[1]);
              tuple.add(item);
            }
            else { // For now do ArrayList
              ArrayList list = new ArrayList(vals.length);
              for (int i = 1; i < vals.length; i++) {
                list.add(vals[i]);
              }
              HashMap<String, Object> item = new HashMap<String, Object>(1);
              item.put(vals[0], list);
              tuple.add(item);
            }
          }
        }
        else {
          tuple.add(t);
        }
      }
      else {
        ; // is it an error token, emit error?
      }
    }
    emit(tuple);
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
