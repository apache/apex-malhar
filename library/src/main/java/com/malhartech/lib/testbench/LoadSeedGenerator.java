/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractInputModule;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates one time seed load based on range provided for the keys, and adds new classification to incoming keys. The new tuple is emitted
 * on the output port <b>data</b>
 * <br>
 * Examples of getting seed distributions include<br>
 * Clients data of a company for every clientId (key is clienId)<br>
 * Persons age, gender, for every phone number (key is phone number)<br>
 * Year, color, mileage for every car make (key is car make model)<br>
 * <br>
 * The classification to be done is based on the value of the property <b>key</b>. This property provides all the classification
 * information and their ranges<br>The range of values for the key is given by properties key_min and key_max
 * <br>
 * Benchmarks: This node has been benchmarked at over ?? million tuples/second in local/in-line mode<br>
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
 * <br>key</b>: Classifier keys to be inserted randomly. Format is "key1,key1start, key1end; key2, key2start, key2end;..."
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
    @PortAnnotation(name = LoadSeedGenerator.OPORT_DATA, type = PortAnnotation.PortType.OUTPUT)
})
public class LoadSeedGenerator extends AbstractInputModule {
    public static final String OPORT_DATA = "data";
    private static Logger LOG = LoggerFactory.getLogger(LoadSeedGenerator.class);

    /**
     * Data for classification values
     */
    ArrayList<String> keys = null;
    ArrayList<Integer> keys_min = null;
    ArrayList<Integer> keys_range = null;
    boolean isstringschema = false;

    final int s_start_default = 0;
    final int s_end_default = 99;

    int s_start = 0;
    int s_end = 99;

    private final Random random = new Random();
    protected boolean alive = true;

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
   * keys are ';' separated list of keys to classify the incoming keys in in_data stream<p>
   *
   */
  public static final String KEY_KEYS = "keys";

  /**
   * If specified as "true" a String class is sent, else HashMap is sent
   */
  public static final String KEY_STRING_SCHEMA = "string_schema";

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

  public HashMap<String, Object> getTuple(int i)
  {
    HashMap<String, Object> ret = new HashMap<String, Object>();

    String key;
    key = Integer.toString(i);

    if (keys == null) {
      ret.put(key, null);
      return ret;
    }

    ArrayList alist = null;
    String str = new String();
    int j = 0;
    for (String s: keys) {
      if (!isstringschema) {
        if (alist == null) {
          alist = new ArrayList();
        }
        alist.add(new valueData(s, new Integer(keys_min.get(j) + random.nextInt(keys_range.get(j)))));
      }
      else {
        if (!str.isEmpty()) {
          str += ';';
        }
        str += s + ":" + Integer.toString(keys_min.get(j) + random.nextInt(keys_range.get(j)));
      }
      j++;
    }

    if (isstringschema) {
      ret.put(key, str);
    }
    else {
      ret.put(key, alist);
    }
    return ret;
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
    if (keys ==null) {
      keys = new ArrayList<String>();
      keys_min = new ArrayList<Integer>();
      keys_range = new ArrayList<Integer>();
    }

    keys.add(key);
    keys_min.add(low);
    keys_range.add(high-low+1);
  }

  /**
   *
   * Code to be moved to a proper base method name
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(ModuleConfiguration config)
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

    String kstr = config.get(KEY_KEYS, "");
    if (kstr.isEmpty()) {
      return ret;
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
            int low = 0;
            int high = 0;
            try {
              low = Integer.parseInt(istr[0]);
              high = Integer.parseInt(istr[1]);
            }
            catch (NumberFormatException e) {
              ret = false;
              throw new IllegalArgumentException(String.format("Weight string should be an integer(%s)", s));
            }
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
  public void setup(ModuleConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new FailedOperationException("validation failed");
    }

    isstringschema = config.getBoolean(KEY_STRING_SCHEMA, false);
    String kstr = config.get(KEY_KEYS, "");
    s_start = config.getInt(KEY_SEED_START, s_start_default);
    s_end = config.getInt(KEY_SEED_END, s_end_default);

    LOG.debug(String.format("Set up for seed_start(%d), seed_end (%d) and keys (\"%s\")", s_start, s_end, kstr));

     // The key format is "str:int,int;str:int,int;..."
    // The format is already validated by myValidation
    if (!kstr.isEmpty()) {
      String[] strs = kstr.split(";");
      for (String s: strs) {
        String[] twostr = s.split(":");
        String key = twostr[0];
        String[] istr = twostr[1].split(",");
        addKeyData(twostr[0], Integer.parseInt(istr[0]), Integer.parseInt(istr[1]));
      }
    }
  }
  @Override
  public void run()
  {
    int lstart = s_start;
    int lend = s_end;

    while (alive) {
      if (lstart < lend) {
        for (int i = lstart; i < lend; i++) {
          emit(OPORT_DATA, getTuple(i));
        }
      }
      else {
        for (int i = lstart; i > lend; i--) {
          emit(OPORT_DATA, getTuple(i));
        }
      }
      // Seed only once
      break;
    }
    LOG.debug("Finished generating tuples");
  }


  /**
   * Stops any more emitting of tuples
   */
  @Override
  public void teardown() {
    this.alive = false; // TODO: need solution for alive in AbstractInputModule
    super.teardown();
  }


  /**
   * Nothing to do as of now in endWindow. But may send out tuples data on endWindow to another port(?)
   */
  @Override
  public void endWindow() {
    // do nothing for now
    // In future support tuple blasting
    // May need to make LoadGenerator thread/tuple_blast/sleep_time a base class
  }
}
