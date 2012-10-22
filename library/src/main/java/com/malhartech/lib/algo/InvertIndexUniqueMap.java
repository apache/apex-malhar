/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.FailedOperationException;
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
 * Takes in a stream via input port "data". Inverts the map and sends out the tuple on output port "index".<br>
 * Takes in specific queries on query port and outputs the data in the cache through console port on receiving the tuple
 * and on each subsequent end_of_window tuple<p>
 * THIS OPERATOR NEEDS TO BE TEMPLATIZED<br>
 *
 *
 *
 *
 * @author amol<br>
 *
 */

public class InvertIndexUniqueMap extends BaseOperator
{
  private static Logger log = LoggerFactory.getLogger(InvertIndexUniqueMap.class);
  public final transient DefaultInputPort<HashMap<String, String>> data = new DefaultInputPort<HashMap<String, String>>(this)
  {
    @Override
    public void process(HashMap<String, String> tuple)
    {
      for (Map.Entry<String, String> e: tuple.entrySet()) {
        HashMap<String, Object> values = map.get(e.getValue());
        if (values == null) {
          values = new HashMap<String, Object>(4); // start with 4 slots, keep it low
          map.put(e.getValue(), values);
        }
        values.put(e.getKey(), null);

        // Now remove the key from old map value
        String cur_key = secondary_index.get(e.getKey());
        if ((cur_key != null) && !cur_key.equals(e.getValue())) { // remove from old map
          values = map.get(cur_key);
          if (values != null) { // must be true
            values.remove(e.getKey());
          }
          if (values.isEmpty()) { // clean up memory if need be
            map.remove(cur_key);
          }
        }
        secondary_index.put(e.getKey(), e.getValue());
      }
    }
  };

  public final transient DefaultInputPort<HashMap<String, String>> query = new DefaultInputPort<HashMap<String, String>>(this)
  {
    @Override
    public void process(HashMap<String, String> tuple)
    {
      if (!console.isConnected()) { // maybe emit an error tuple
        return;
      }
      String qid = null;
      String phone = null;
      String location = null;
      for (Map.Entry<String, String> e: tuple.entrySet()) {
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
            log.debug(String.format("Removing query id \"%s\" as a location", qid));
          }
          if (phone.isEmpty()) { // simply remove the channel
            if (phone_register.get(qid) != null) {
              phone_register.remove(qid);
              log.debug(String.format("Removing query id \"%s\"", qid));
            }
          }
          else { // register the phone channel
            phone_register.put(qid, phone);
            emitConsoleTuple(qid, true);
            log.debug(String.format("Registered query id \"%s\", with phonenum \"%s\"", qid, phone));
          }
        }
        else if (locationchannel) {
          if (phone_register.get(qid) != null) { // check if user is moving from a phone query to a location query
            phone_register.remove(qid);
            log.debug(String.format("Removing query id \"%s\" as a phone", qid));
          }
          if (location.isEmpty()) { // simply remove the channel
            if (location_register.get(qid) != null) {
              location_register.remove(qid);
              log.debug(String.format("Removing query id \"%s\"", qid));
            }
          }
          else {
            location_register.put(qid, location);
            emitConsoleTuple(qid, false);
            log.info(String.format("Registered query id \"%s\", with location \"%s\"", qid, location));
          }
        }
      }

    }
  };

  public final transient DefaultOutputPort<HashMap<String, ArrayList>> index = new DefaultOutputPort<HashMap<String, ArrayList>>(this);
  public final transient DefaultOutputPort<HashMap<String, Object>> console = new DefaultOutputPort<HashMap<String, Object>>(this);

  public static final String KEY_SEED_QUERYS_JSON = "seedQueries";
  HashMap<String, HashMap<String, Object>> map = new HashMap<String, HashMap<String, Object>>();
  HashMap<String, String> secondary_index = new HashMap<String, String>(5);
  HashMap<String, String> phone_register = new HashMap<String, String>(5);
  HashMap<String, String> location_register = new HashMap<String, String>(5);
  public static final String CHANNEL_PHONE = "phone";
  public static final String CHANNEL_LOCATION = "location";
  public static final String IDENTIFIER_CHANNEL = "queryId";

  String seedquery = "";

  public void setSeedquery(String str)
  {
    parseSeedQueries(str);

  }

  protected boolean hasIndex(String key)
  {
    HashMap<String, Object> val = map.get(key);
    return (val != null) && !val.isEmpty();
  }

  protected boolean hasSecondaryIndex(String key)
  {
    return (secondary_index.get(key) != null);
  }

  protected void emitConsoleTuple(String id, boolean isphone)
  {
    if (!console.isConnected()) {
      return;
    }
    String key = isphone ? phone_register.get(id) : location_register.get(id);
    if (key == null) { // something awful? bad data?
      return;
    }

    HashMap<String, Object> tuples = new HashMap<String, Object>(3);
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
      HashMap<String, Object> values = map.get(key);
      ArrayList<String> phonelist = new ArrayList<String>();
      if (values != null) {
        for (Map.Entry<String, Object> e: values.entrySet()) {
          phonelist.add(e.getKey());
        }
      }
      tuples.put(CHANNEL_PHONE, phonelist);
    }
    console.emit(tuples);
  }

  private void parseSeedQueries(String s)
  {
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
    }
    catch (JSONException e) {
      throw new FailedOperationException(e);
    }
  }


  /**
   * Emit all the data and clear the hash
   */
  @Override
  public void endWindow()
  {
    if (console.isConnected()) {
      for (Map.Entry<String, String> e: phone_register.entrySet()) {
        emitConsoleTuple(e.getKey(), true);
      }
      for (Map.Entry<String, String> e: location_register.entrySet()) {
        emitConsoleTuple(e.getKey(), false);
      }
    }

    if (index.isConnected()) {
      for (Map.Entry<String, HashMap<String, Object>> e: map.entrySet()) {
        ArrayList keys = new ArrayList();
        for (Map.Entry<String, Object> o : e.getValue().entrySet()) {
          keys.add(o.getKey());
        }
        HashMap<String, ArrayList> tuple = new HashMap<String, ArrayList>(1);
        tuple.put(e.getKey(), keys);
        index.emit(tuple);
      }
    }
  }
}
