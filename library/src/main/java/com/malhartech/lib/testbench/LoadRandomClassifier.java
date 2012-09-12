/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.NodeConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a in stream <b>in_data</b> and adds new classification to incoming keys. The new tuple is emitted
 * on the output port <b>out_data</b>
 * <br>
 * Examples of getting random distributions include<br>
 * Clients of a company<br>
 * Persons age, location<br>
 * <br>
 * The classification to be done is based on the value of the property <b>key</b>. This property provides all the classification
 * information and their ranges<br>Sticky classification can be done by setting the sticky property to true. This property ensures
 * that an incoming key is classified only once<br>
 * <br>
 * Benchmarks: This node has been benchmarked at over ?? million tuples/second in local/inline mode<br>
 * <br>
 * <b>Default schema</b>:<br>
 * Schema for port <b>in_data</b>: The schema is an ArrayList<String><br>
 * Schema for port <b>out_data</b>: The schema is HashMap<String, ArrayList<valueData>>, where valueData is class{String, Integer}<br>
 * <b><String> schema<b>:<br>
 * Schema for port <b>in_data</b>: Multiple strings are separated by comma (or delimiter)<br>
 * Schema for port <b>out_data</b>: The string is "key;valkey1:value1;valkey2:value2;..."<br>
 * <br>
 * <b>Port Interface</b><br>
 * <b>out_data</b>: Output port for emitting the new classified tuple<br>
 * <b>in_data</b>: Input port for receiving the incoming tuple<br>
 * <br>
 * <b>Properties</b>:
 * <b>keys</b>: Is a comma separated list of keys to be inserted <br>
 * <br>string_schema</b>: If set to true, operates in string schema mode<br>
 * <br>delimiter</b>: Separates incoming keys in string schema mode<br>
 * <br>
 * Compile time checks are:<br>
 * <b>keys</b> cannot be empty. Keys have to be formatted as "valkey1:val1_low,val1_high;valkey2:val2_low,val2_high;..."<br>
 * <br>
 * @author amol
 */
@NodeAnnotation(
        ports = {
    @PortAnnotation(name = LoadRandomClassifier.IPORT_IN_DATA, type = PortAnnotation.PortType.INPUT),
    @PortAnnotation(name = LoadRandomClassifier.OPORT_OUT_DATA, type = PortAnnotation.PortType.OUTPUT)
})
public class LoadRandomClassifier extends AbstractNode {
    public static final String IPORT_IN_DATA = "in_data";
    public static final String OPORT_OUT_DATA = "out_data";
    private static Logger LOG = LoggerFactory.getLogger(LoadRandomClassifier.class);

    HashMap<String, Object> processedkeys = new HashMap<String, Object>();
    ArrayList<String> keys = new ArrayList<String>();
    ArrayList<Integer> keys_min = new ArrayList<Integer>();
    ArrayList<Integer> keys_max = new ArrayList<Integer>();
    boolean isstringschema = false;
  /**
   * keys are ';' separated list of keys to classify the incoming keys in in_data stream<p>
   *
   */
  public static final String KEY_KEYS = "keys";
  /**
   * If specified as "true" a String class is sent, else HashMap is sent
   */
  public static final String KEY_STRING_SCHEMA = "string_schema";

  private Random random = new Random();

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


  /**
   *
   * Inserts a tuple for a given outbound key
   * @param tuples bag of tuples
   * @param key the key
   */

  private void insertTuple(HashMap<String, ArrayList<valueData>> tuples, String key)
  {
    ArrayList<valueData> val = new ArrayList<valueData>();
    int j = 0;
    for (String s : keys) {
      val.add(new valueData(s, new Integer(keys_min.get(j) + random.nextInt(keys_max.get(j) - keys_min.get(j)))));
      j++;
    }
    tuples.put(key, val);
  }

  /**
   *
   * Add a key data. By making a single call we ensure that all three Arrays are not corrupt and that the addition is atomic/one place
   * @param key
   * @param low
   * @param high
   */
  void addKeyData(String key, int low, int high)
  {
    keys.add(key);
    keys_min.add(low);
    keys_max.add(high);
  }

  /**
   *
   * Code to be moved to a proper base method name
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(NodeConfiguration config)
  {
    boolean ret = true;
    String kstr = config.get(KEY_KEYS, "");

    if (kstr.isEmpty()) {
      ret = false;
      throw new IllegalArgumentException("Parameter \"key\" is empty");
    }
    else {
      LOG.debug(String.format("The key is %s", kstr));
    }

    // The key format is "str:int,int;str:int,int;..."
    String[] strs = kstr.split(";");
    int j = 0;
    for (String s: strs) {
      j++;
      if (s.isEmpty()) {
        ret = false;
        throw new IllegalArgumentException(String.format("%d slot of parameter \"key\" is empty", j));
      }
      else {
        String[] twostr = s.split(":");
        if (twostr.length != 2) {
          ret = false;
          throw new IllegalArgumentException(String.format("\"%s\" malformed in parameter \"key\"", s));
        }
        else {
          String key = twostr[0];
          String[] istr = twostr[1].split(",");
          if (istr.length != 2) {
            ret = false;
            throw new IllegalArgumentException(String.format("Values \"%s\" for \"%s\" of parameter \"key\" is malformed", twostr[1], key));
          }
          else {
            int low = Integer.parseInt(istr[0]);
            int high = Integer.parseInt(istr[1]);
            if (low >= high) {
              ret = false;
              throw new IllegalArgumentException(String.format("Low value \"%d\" is >= high value \"%d\" for \"%s\"", low, high, key));
            }
          }
        }
      }
    }
    return ret;
  }

  /**
   * Sets up all the config parameters. Assumes checking is done and has passed
   *
   * @param config
   */
  @Override
  public void setup(NodeConfiguration config)
  {
    super.setup(config);
    if (!myValidation(config)) {
      throw new IllegalArgumentException("Did not pass validation");
    }

    isstringschema = config.getBoolean(KEY_STRING_SCHEMA, false);
    String kstr = config.get(KEY_KEYS, "");

    // The key format is "str:int,int;str:int,int;..."
    // The format is already validated by myValidation
    String[] strs = kstr.split(";");
    for (String s: strs) {
      String[] twostr = s.split(":");
      String key = twostr[0];
      String[] istr = twostr[1].split(",");
      addKeyData(twostr[0], Integer.parseInt(istr[0]), Integer.parseInt(istr[1]));
    }
  }

   /**
     * Process each tuple
     *
     * @param payload
     */
    @Override
    public void process(Object payload) {
      HashMap<String, ArrayList<valueData>> tuples = new HashMap<String, ArrayList<valueData>>();

      if (isstringschema) {
        insertTuple(tuples, (String) payload);
      }
      else {
        for (String ikey : (ArrayList<String>) payload) {
          insertTuple(tuples, ikey);
        }
      }
      emit(OPORT_OUT_DATA, tuples);
}

  /**
   *
   * Checks for user specific configuration values<p>
   *
   * @param config
   * @return boolean
   */
  @Override
  public boolean checkConfiguration(NodeConfiguration config)
  {
    boolean ret = true;
    // TBD
    return ret && super.checkConfiguration(config);
  }
}
