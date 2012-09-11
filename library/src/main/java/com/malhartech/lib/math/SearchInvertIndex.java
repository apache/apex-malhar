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
    HashMap<String, ArrayList<String>> index = null;

    class valueData {
      String str;
      Object value;
    }
    HashMap<String, ArrayList<valueData>> vindex = null;
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
   * Takes in a key and an arrayIndex. ReverseIndexes the strings in the ArrayIndex
   * @param payload
   */
    @Override
    public void process(Object payload) {

      // Code needs to be reused as almost exactly same code is in both if and else
      if (passvalue) {
        HashMap<String, ArrayList<valueData>> intuples = (HashMap<String, ArrayList<valueData>>) payload;
        for (Map.Entry<String, ArrayList<valueData>> e : intuples.entrySet()) {
          ArrayList<valueData> alist = e.getValue();
          if (alist != null) {
            Iterator<valueData> values = alist.iterator();
            while (values.hasNext()) {
              valueData ival = values.next();
              String key = ival.str;
              ArrayList<valueData> val = vindex.get(key);
              if (val == null) {
                val = new ArrayList<valueData>();
              }
              valueData tval = new valueData();
              tval.str = e.getKey();
              tval.value =ival.value;
              val.add(tval);
              vindex.put(key, val);
              //vindex.put(key, val); // not needed except when val was null
            }
          }
          else { // bad tuple
            // emitError() here
          }
        }
      }
      else {
        HashMap<String, ArrayList<String>> intuples = (HashMap<String, ArrayList<String>>) payload;
        for (Map.Entry<String, ArrayList<String>> e: intuples.entrySet()) {
          ArrayList<String> alist = e.getValue();
          if (alist != null) {
            Iterator<String> values = alist.iterator();
            while (values.hasNext()) {
              String key = values.next();
              ArrayList<String> val = index.get(key);
              if (val == null) {
                val = new ArrayList<String>();
              }
              val.add(e.getKey());
              index.put(key, val); // not needed except when val was null
            }
          }
          else { // bad tuple
            // emitError() here
          }
        }
      }
    }

    /**
     *
     * @param config
     * @return
     */
    public boolean myValidation(NodeConfiguration config) {
      // No checks are needed for now
        return true;
    }

  @Override
  public void setup(NodeConfiguration config) {
    passvalue = config.getBoolean(KEY_PASSVALUE, false);
    if (passvalue) {
      vindex = new HashMap<String, ArrayList<valueData>>();
    }
    else {
      index = new HashMap<String, ArrayList<String>>();
    }
    super.setup(config);
  }


  /**
   * Emit all the data and clear the hash
   */
  @Override
  public void endWindow()
  {
    if (passvalue) {
      for (Map.Entry<String, ArrayList<valueData>> e: vindex.entrySet()) {
        emit(e);
      }
      vindex.clear();
    }
    else {
      for (Map.Entry<String, ArrayList<String>> e: index.entrySet()) {
        emit(e);
      }
      index.clear();
    }
    super.endWindow();
  }
}
