/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.NodeConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in a stream via input port "data". Inverts the index and sends out the tuple on output port "index"<p>At the
 *
 *
 *
 * @author amol<br>
 *
 */
@NodeAnnotation(
        ports = {
    @PortAnnotation(name = SearchInvertIndex.IPORT_DATA, type = PortType.INPUT),
    @PortAnnotation(name = SearchInvertIndex.OPORT_INDEX, type = PortType.OUTPUT)
})
public class SearchInvertIndex extends AbstractNode {

    public static final String IPORT_DATA = "data";
    public static final String OPORT_INDEX = "index";
    private static Logger LOG = LoggerFactory.getLogger(SearchInvertIndex.class);
    HashMap<String, ArrayList> index = null;

    class valueData {
      String str;
      Object value;

      valueData(String istr, Object val) {
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
   * @param key
   * @return ArrayList
   */
  ArrayList getArrayList(String key)
  {
    ArrayList ret = index.get(key);
    if (ret == null) {
      if (passvalue) {
        ret = new ArrayList<valueData>();
      }
      else {
        ret = new ArrayList<String>();
      }
      index.put(key, ret);
    }
    return ret;
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
          Object avalue = null;
          if (passvalue) {
            valueData dval =  (valueData) values.next();
            key = dval.str;
            avalue = new valueData(e.getKey(), dval.value);
          }
          else {
            key = (String) values.next();
            avalue = e.getKey();
          }
          getArrayList(key).add(avalue);
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
    public boolean myValidation(NodeConfiguration config) {
      // No checks are needed for now
        return true;
    }

    /**
     *
     * @param config
     */
  @Override
  public void setup(NodeConfiguration config) {
    passvalue = config.getBoolean(KEY_PASSVALUE, false);
    index = new HashMap<String, ArrayList>();
    super.setup(config);
  }


  /**
   * Emit all the data and clear the hash
   */
  @Override
  public void endWindow()
  {
    if (passvalue) {
      for (Map.Entry<String, ArrayList> e: index.entrySet()) {
        emit(e);
      }
      index.clear();
    }
    super.endWindow();
  }
}
