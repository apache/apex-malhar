/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.GenericNode;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.OperatorConfiguration;
import com.malhartech.api.Sink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
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
public class InvertIndexMap extends GenericNode
{
  private static Logger LOG = LoggerFactory.getLogger(InvertIndexMap.class);

  public static final String IPORT_DATA = "data";
  public static final String IPORT_QUERY = "query";
  public static final String OPORT_INDEX = "index";
  public static final String OPORT_CONSOLE = "index";

  public static final String KEY_SEED_QUERYS_JSON = "seedQueries";

  HashMap<String, HashMap<String, Object>> index = null;
  HashMap<String, String> secondary_index = null;
  HashMap<String, String> phone_register = null;
  HashMap<String, String> location_register = null;

  public static final String CHANNEL_PHONE = "phone";
  public static final String CHANNEL_LOCATION = "location";
  public static final String IDENTIFIER_CHANNEL = "queryId";

  boolean console_connected = false;
  boolean index_connected = false;

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
        String location = null;
        for (Map.Entry<String, String> e: ((HashMap<String, String>)payload).entrySet()) {
          if (e.getKey().equals(IDENTIFIER_CHANNEL)) {
            qid = e.getValue();
          }
          else if (e.getKey().equals(CHANNEL_PHONE)) {
            phone = e.getValue();
          }
          else if (e.getKey().equals(CHANNEL_LOCATION)) {
            location = e.getValue();
          }
        }
        boolean phonechannel = (phone != null);
        boolean locationchannel = (location != null);
        boolean hasqid = (qid != null);

        if (hasqid) { // without qid, ignore
          if (phonechannel) {
            if (location_register.get(qid) != null) { // check if user is moving from a location query to a phone query
              location_register.remove(qid);
              LOG.debug(String.format("Removing query id \"%s\" as a location", qid));
            }
            if (phone.isEmpty()) { // simply remove the channel
              if (phone_register.get(qid) != null) {
                phone_register.remove(qid);
                LOG.debug(String.format("Removing query id \"%s\"", qid));
              }
            }
            else { // register the phone channel
              phone_register.put(qid, phone);
              emitConsoleTuple(qid, true);
              LOG.debug(String.format("Registered query id \"%s\", with phonenum \"%s\"", qid, phone));
            }
          }
          else if (locationchannel) {
            if (phone_register.get(qid) != null) { // check if user is moving from a phone query to a location query
              phone_register.remove(qid);
              LOG.debug(String.format("Removing query id \"%s\" as a phone", qid));
            }
            if (location.isEmpty()) { // simply remove the channel
              if (location_register.get(qid) != null) {
                location_register.remove(qid);
                LOG.debug(String.format("Removing query id \"%s\"", qid));
              }
            }
            else {
              location_register.put(qid, location);
              emitConsoleTuple(qid, false);
              LOG.info(String.format("Registered query id \"%s\", with location \"%s\"", qid, location));
            }
          }
        }
      }
      else { // should give an error tuple as a query port was sent without console connected
        LOG.warn("Received invalid query {}", payload);
      }
    }
  }

  protected void emitConsoleTuple(String id, boolean isphone) {
    if (!console_connected) {
      return;
    }

    String key = isphone ? phone_register.get(id) : location_register.get(id);
    if (key == null) { // something awful? bad data?
      return;
    }

    HashMap<String,Object> tuples = new HashMap<String, Object>(3);
    tuples.put(IDENTIFIER_CHANNEL, id);

    if (isphone) {
      String val = secondary_index.get(key);
      if (val == null) {
        val = "Not Found,Not Found";
      }
      tuples.put(CHANNEL_PHONE, key);
      tuples.put(CHANNEL_LOCATION, val);
    }
    else {
      tuples.put(CHANNEL_LOCATION, key);
      HashMap<String, Object> values = index.get(key);
      ArrayList<String> phonelist = new ArrayList<String>();
      if (values != null) {
        for (Map.Entry<String, Object> e: values.entrySet()) {
          phonelist.add(e.getKey());
        }
      }
      tuples.put(CHANNEL_PHONE, phonelist);
    }
    emit(OPORT_CONSOLE, tuples);
  }

  /**
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(OperatorConfiguration config)
  {
    // no checks as of now
    return true;
  }

  private void parseSeedQueries(String s) {
    try {
      JSONObject queries = new JSONObject(s);
      if (queries.has(CHANNEL_PHONE)) {
        JSONObject json = queries.getJSONObject(CHANNEL_PHONE);
        Iterator<?> it = json.keys();
        while (it.hasNext()) {
          String key = (String)it.next();
          String val = json.getString(key);
          if (val != null) {
            phone_register.put(key, val);
          }
        }
      }
      if (queries.has(CHANNEL_LOCATION)) {
        JSONObject json = queries.getJSONObject(CHANNEL_LOCATION);
        Iterator<?> it = json.keys();
        while (it.hasNext()) {
          String key = (String)it.next();
          String val = json.getString(key);
          if (val != null) {
            location_register.put(key, val);
          }
        }
      }
    } catch (JSONException e) {
      throw new FailedOperationException(e);
    }
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
    index = new HashMap<String, HashMap<String, Object>>();
    secondary_index = new HashMap<String, String>(5);
    phone_register = new HashMap<String, String>(5);
    location_register = new HashMap<String, String>(5);

    String seedQueries = config.get(KEY_SEED_QUERYS_JSON);
    if (seedQueries != null) {
      parseSeedQueries(seedQueries);
    }
    //location_register.put("loc1", "34,87");
    //phone_register.put("blah", "9905500");
    //phone_register.put("id1002", "9999998");
  }

  /**
   * Emit all the data and clear the hash
   */
  @Override
  public void endWindow()
  {
    if (console_connected) {
      for (Map.Entry<String, String> e: phone_register.entrySet()) {
          emitConsoleTuple(e.getKey(), true);
      }
      for (Map.Entry<String, String> e: location_register.entrySet()) {
          emitConsoleTuple(e.getKey(), false);
      }
    }
    if (index_connected) {
      for (Map.Entry<String, HashMap<String, Object>> e: index.entrySet()) {
        emit(e.getKey(), e.getValue());
      }
    }
  }
}
