/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.GenericNode;
import com.malhartech.api.FailedOperationException;
import com.malhartech.api.OperatorConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
  @PortAnnotation(name = InvertIndexArray.IPORT_DATA, type = PortType.INPUT),
  @PortAnnotation(name = InvertIndexArray.OPORT_INDEX, type = PortType.OUTPUT)
})
public class InvertIndexArray extends GenericNode
{
  public static final String IPORT_DATA = "data";
  public static final String OPORT_INDEX = "index";
  private static Logger LOG = LoggerFactory.getLogger(InvertIndexArray.class);
  HashMap<String, ArrayList> index = null;

  class valueData
  {
    String str;
    Object value;

    valueData(String istr, Object val)
    {
      str = istr;
      value = val;
    }
  }
  boolean passvalue = false;
  /**
   *
   * The incoming tuple is an ArrayList.
   * If hasvalue is true then it is an array of a pairs of String and an Object
   * if hasvalue is false then it is an array of Strings
   *
   */
  public static final String KEY_PASSVALUE = "passvalue";

  /**
   *
   * Returns the ArrayList stored for a key
   *
   * @param key
   * @return ArrayList
   */
  void insert(String key, Object value)
  {
    ArrayList list = index.get(key);
    if (list == null) {
      if (passvalue) {
        list = new ArrayList<valueData>();
      }
      else {
        list = new ArrayList<String>();
      }
      index.put(key, list);
    }
    list.add(value);
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
    HashMap<String, ArrayList<Object>> intuples = (HashMap<String, ArrayList<Object>>)payload;
    for (Map.Entry<String, ArrayList<Object>> e: intuples.entrySet()) {
      ArrayList<Object> alist = e.getValue();
      if (alist != null) {
        Iterator<Object> values = alist.iterator();
        while (values.hasNext()) {
          String key = null;
          Object value = null;
          if (passvalue) {
            valueData dval = (valueData)values.next();
            key = dval.str;
            value = new valueData(e.getKey(), dval.value);
          }
          else {
            key = (String)values.next();
            value = e.getKey();
          }
          insert(key, value);
        }
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
  public boolean myValidation(OperatorConfiguration config)
  {
    // No checks for passing a boolean value
    return true;
  }

  /**
   *
   * @param config
   */
  @Override
  public void setup(OperatorConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new FailedOperationException("Did not pass validation");
    }
    passvalue = config.getBoolean(KEY_PASSVALUE, false);
    index = new HashMap<String, ArrayList>();
  }

  /**
   * Emit all the data and clear the hash
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<String, ArrayList> e: index.entrySet()) {
      HashMap<String, ArrayList> tuple = new HashMap<String, ArrayList>();
      tuple.put(e.getKey(), e.getValue());
      emit(tuple);
    }
    index.clear();
    super.endWindow();
  }
}
