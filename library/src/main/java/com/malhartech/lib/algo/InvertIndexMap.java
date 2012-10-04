/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.Sink;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in a stream via input port "data". Inverts the index and sends out the tuple on output port "index". Takes in specific queries on query port
 * and outputs the data in the cache through console port on receiving the tuple and on each subsequent end_of_window tuple<p>
 *
 *
 *
 *
 * @author amol<br>
 *
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = InvertIndexMap.IPORT_DATA, type = PortType.INPUT),
  @PortAnnotation(name = InvertIndexMap.IPORT_QUERY, type = PortType.INPUT),
  @PortAnnotation(name = InvertIndexMap.OPORT_INDEX, type = PortType.OUTPUT),
  @PortAnnotation(name = InvertIndexMap.OPORT_CONSOLE, type = PortType.OUTPUT)
})
public class InvertIndexMap extends AbstractModule
{
  public static final String IPORT_DATA = "data";
  public static final String IPORT_QUERY = "query";
  public static final String OPORT_INDEX = "index";
  public static final String OPORT_CONSOLE = "index";
  private static Logger LOG = LoggerFactory.getLogger(InvertIndexMap.class);

  HashMap<String, HashMap<String, Object>> index = null;
  HashMap<String, String> secondary_index = null;
  HashMap<String, String> query_register = null;

  boolean console_connected = false;
  boolean index_connected = false;

  int tcount = 1;

  protected boolean hasIndex(String key) {
    HashMap<String, Object> val = index.get(key);
    return (val != null) && !val.isEmpty();
  }

  protected boolean hasSecondaryIndex(String key) {
    return (secondary_index.get(key) != null);
  }


  /**
   *
   * @param id
   * @param dagpart
   */
  @Override
  public void connected(String id, Sink dagpart)
  {
    if (id.equals(OPORT_CONSOLE)) {
      console_connected = (dagpart != null);
    }
    else if (id.equals(OPORT_INDEX)) {
      index_connected = (dagpart != null);
    }
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
    if (IPORT_DATA.equals(getActivePort())) {
      for (Map.Entry<String, String> e: ((HashMap<String, String>) payload).entrySet()) {
        tcount++;
        HashMap<String, Object> values = index.get(e.getValue());
        if (values == null) {
          values = new HashMap<String, Object>(4); // start with 4 slots, keep it low
          index.put(e.getValue(), values);
        }
        values.put(e.getKey(), null);

        // Now remove the key from old index value
        String cur_key = secondary_index.get(e.getKey());
        if ((cur_key != null) && !cur_key.equals(e.getValue())) { // remove from old index
          values = index.get(cur_key);
          if (values != null) { // must be true
            values.remove(e.getKey());
          }
          if (values.isEmpty()) { // clean up memory if need be
            index.remove(cur_key);
          }
        }
        secondary_index.put(e.getKey(), e.getValue());
      }
    }
    else if (IPORT_QUERY.equals(getActivePort())) {
      if (console_connected) {
        String qid = null;
        String phone = null;
        for (Map.Entry<String, String> e: ((HashMap<String, String>)payload).entrySet()) {
          if (e.getKey().equals("queryid")) {
            qid = e.getValue();
          }
          else if (e.getKey().equals("phone")) {
            phone = e.getValue();
          }
        }
        if ((phone == null) || phone.isEmpty()) {
          if ((qid != null) && !qid.isEmpty()) {
            query_register.remove(qid);
          }
        }
        else if ((qid != null) && !qid.isEmpty()) {
          query_register.put(qid, phone);
          emitConsoleTuple(qid, phone);
        }
      }
      else { // should give an error tuple as a query port was sent without console connected
      }
    }
  }

  protected void emitConsoleTuple(String id, String key) {
    String val = secondary_index.get(key);
    if (val == null) {
      val = "Not Found,Not Found";
    }
    HashMap<String,String> tuples = new HashMap<String,String>(3);
    tuples.put("queryId", id);
    tuples.put("phone", key);
    tuples.put("location", val);
    emit(OPORT_CONSOLE, tuples);
  }

  /**
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(ModuleConfiguration config)
  {
    // no checks as of now
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
    secondary_index = new HashMap<String, String>();
    query_register = new HashMap<String, String>();

    query_register.put("id2201", "9042031");
    query_register.put("id1000", "9000020");
    query_register.put("id1001", "9005000");
    query_register.put("id1002", "9005500");
    query_register.put("id1002", "9999998");
  }

  /**
   * Emit all the data and clear the hash
   */
  @Override
  public void endWindow()
  {
    if (console_connected) {
      for (Map.Entry<String, String> e: query_register.entrySet()) {
        emitConsoleTuple(e.getKey(), e.getValue());
      }
    }
    else if (index_connected) {
      // Todo, send out entire index
    }
  }
}
