/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in a stream via input port "data". Inverts the index and sends out the tuple on output port "index"<p>
 *
 *
 *
 * @author amol<br>
 *
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = InvertIndexMap.IPORT_DATA, type = PortType.INPUT),
  @PortAnnotation(name = InvertIndexMap.OPORT_INDEX, type = PortType.OUTPUT)
})
public class InvertIndexMap extends AbstractModule
{
  public static final String IPORT_DATA = "data";
  public static final String OPORT_INDEX = "index";
  private static Logger LOG = LoggerFactory.getLogger(InvertIndexMap.class);
  HashMap<String, HashMap<String, Object>> index = null;

 /**
   *
   * Returns the ArrayList stored for a key
   *
   * @param key
   * @return ArrayList
   */
  void insert(String key, Object value)
  {
  }

  /**
   *
   * Takes in a key and an arrayIndex. ReverseIndexes the strings in the ArrayIndex
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    HashMap<String, Object> intuples = (HashMap<String, Object>)payload;
    for (Map.Entry<String, Object> e: intuples.entrySet()) {
      String val = (String)e.getValue();
      if (val != null && !val.isEmpty()) {
        insert(val, e.getKey());
      }
      else { // bad tuple
        // emitError() here
      }
    }
  }

  /**
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(ModuleConfiguration config)
  {
    // No checks for passing a boolean value
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
      throw new FailedOperationException("Did not pass validation");
    }
    index = new HashMap<String, HashMap<String, Object>>();
  }

  /**
   * Emit all the data and clear the hash
   */
  @Override
  public void endWindow()
  {
/*    for (Map.Entry<String, ArrayList> e: index.entrySet()) {
      HashMap<String, ArrayList> tuple = new HashMap<String, ArrayList>();
      tuple.put(e.getKey(), e.getValue());
      emit(tuple);
    }
*/    index.clear();
    super.endWindow();
  }
}
