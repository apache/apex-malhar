/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.Module;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.OperatorConfiguration;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates seeds and merges data as it comes in from input ports (<b>in_data1</b>, and <b>in_data2</b>. The new tuple is emitted
 * on the output port <b>out_data</b>
 * <br>
 * Examples of getting seed distributions include<br>
 * Chages in mobile co-ordinates of a phone<br>
 * Random changes on motion of an object<br>
 * <br>
 * The seed is created from the values of properties <b>seed_start</b>, and <b>seed_end</b>
 * <br>
 * <b>Benchmarks</b>:<br>
 * String: Benchmarked at over 5.9 million tuples/second in local/in-line mode<br>
 * Integer: Benchmarked at over 4.4 million tuples/second in local/in-line mode<br>
 * <br>
 * <b>Default schema</b>:<br>
 * Schema for port <b>data</b>: The default schema is HashMap<String, ArrayList<valueData>>, where valueData is class{String, Integer}<br>
 * <b>String schema</b>: The string is "key;valkey1:value1;valkey2:value2;..."<br>
 * <b>HashMap schema</b>: Key is String, and Value is a ArrrayList<String, Number><br>
 * The value in both the schemas is an integer (for choice of strings, these are enum values)
 * <br>
 * <b>Port Interface</b><br>
 * <b>data</b>: Output port for emitting the new classified seed<br>
 * <br>
 * <b>Properties</b>:
 * <b>seed_start</b>: An integer for the seed to start from<br>
 * <b>seed_end</b>: An integer for the seed to end with<br>
 * <br>string_schema</b>: If set to true, operates in string schema mode<br>
 * <br>key</b>: Classifier keys to be inserted in order for each incoming tuple. Format is "key1,key1start, key1end; key2, key2start, key2end;..."
 * <br>
 * Compile time checks are:<br>
 * <b>seed_start</b>Has to be an integer<br>
 * <b>sedd_end</b>Has to be an integer<br>
 * <b>key</b>If provided has to be in format "key1,key1start,key1end;key2, key2start, key2end; ..."
 * <br>
 * @author amol
 */
@ModuleAnnotation(
        ports = {
    @PortAnnotation(name = SeedEventClassifier.IPORT_IN_DATA1, type = PortAnnotation.PortType.INPUT),
    @PortAnnotation(name = SeedEventClassifier.IPORT_IN_DATA2, type = PortAnnotation.PortType.INPUT),
    @PortAnnotation(name = SeedEventClassifier.OPORT_OUT_DATA, type = PortAnnotation.PortType.OUTPUT)
})
public class SeedClassifier extends Module {
    public static final String IPORT_IN_DATA1 = "in_data1";
    public static final String IPORT_IN_DATA2 = "in_data2";
    public static final String OPORT_OUT_DATA = "out_data";
    private static Logger LOG = LoggerFactory.getLogger(SeedEventClassifier.class);

    /**
     * Data for classification values
     */
    HashMap<String, Object> keys = new HashMap<String, Object>();
    String indata1_str = new String();
    String indata2_str = new String();

    boolean isstringschema = false;

    final int s_start_default = 0;
    final int s_end_default = 99;

    int s_start = 0;
    int s_end = 99;
    int seed = 0;

  /**
   * Start integer value for seeding<p>
   *
   */
  public static final String KEY_SEED_START = "seed_start";

  /**
   * End integer value for seeding<p>
   *
   */
  public static final String KEY_SEED_END = "seed_end";

  /**
   * Classifer for port in_data1
   */
  public static final String KEY_IN_DATA1_CLASSIFIER = "in_data1_classifier";

  /**
   * Classifer for port in_data2
   */
  public static final String KEY_IN_DATA2_CLASSIFIER = "in_data2_classifier";

  /**
   * If specified as "true" a String class is sent, else HashMap is sent
   */
  public static final String KEY_STRING_SCHEMA = "string_schema";


  /**
   *
   * Code to be moved to a proper base method name
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(OperatorConfiguration config)
  {
    boolean ret = true;

    String seedstart = config.get(KEY_SEED_START, "");
    String seedend = config.get(KEY_SEED_END, "");

    if (seedstart.isEmpty()) {
      if (!seedend.isEmpty()) {
        ret = false;
        throw new IllegalArgumentException(String.format("seedstart is empty, but seedend (%s) is not", seedend));
      }
    }
    else {
      if (seedend.isEmpty()) {
        ret = false;
        throw new IllegalArgumentException(String.format("seedstart is specified (%s), but seedend is empty", seedstart));
      }
      // Both are specified
      int lstart = 0;
      int lend = 0;
      try {
        lstart = Integer.parseInt(seedstart);
      }
      catch (NumberFormatException e) {
        ret = false;
        throw new IllegalArgumentException(String.format("seed_start (%s) should be an integer", seedstart));
      }
      try {
        lend = Integer.parseInt(seedend);
      }
      catch (NumberFormatException e) {
        ret = false;
        throw new IllegalArgumentException(String.format("seed_end (%s) should be an integer", seedend));
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
  public void setup(OperatorConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new FailedOperationException("validation failed");
    }

    isstringschema = config.getBoolean(KEY_STRING_SCHEMA, false);
    int istart = config.getInt(KEY_SEED_START, s_start_default);
    int iend = config.getInt(KEY_SEED_END, s_end_default);
    indata1_str = config.get(KEY_IN_DATA1_CLASSIFIER);
    indata2_str = config.get(KEY_IN_DATA2_CLASSIFIER);

    if (istart > iend) {
      s_start = iend;
      s_end = istart;
    }
    else {
      s_start = istart;
      s_end = iend;
    }
    seed = s_start;
    LOG.debug(String.format("Set up for seed_start(%d), seed_end (%d), indata1_classifier(%s), and indata2_classifier(%s)", s_start, s_end, indata1_str, indata2_str));
  }

  /**
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    String ikey;
    if (IPORT_IN_DATA1.equals(getActivePort())) {
      ikey = indata1_str;
    }
    else {
      ikey = indata2_str;
    }


    if (isstringschema) {
      String tuple = Integer.toString(seed);
      tuple += ":";
      tuple += ikey;
      tuple += ",";
      tuple += (String) payload;
      emit(OPORT_OUT_DATA, tuple);
    }
    else {
      HashMap<String, Object> tuple = new HashMap<String, Object>(1);
      HashMap<String, Object> val = new HashMap<String, Object>(1);
      val.put(ikey, payload);
      tuple.put(Integer.toString(seed), val);
      emit(OPORT_OUT_DATA, tuple);
    }
    seed++;
    if (seed == s_end) {
      seed = s_start;
    }
  }
}
