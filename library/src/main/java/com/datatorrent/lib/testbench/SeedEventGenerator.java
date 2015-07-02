/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.testbench;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.lib.util.KeyValPair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates a one time seed load based on the range provided by the keys,
 * and adds new classification to incoming keys.&nbsp;
 * Generated tuples are emitted on the keyvalpair_list, val_list, string_data, and val_data output ports.
 * <p>
 * Examples of getting seed distributions include<br>
 * Clients data of a company for every clientId (key is clienId)<br>
 * Persons age, gender, for every phone number (key is phone number)<br>
 * Year, color, mileage for every car make (key is car make model)<br>
 * <br>
 * The classification to be done is based on the value of the property <b>key</b>. This property provides all the classification
 * information and their ranges<br>The range of values for the key is given in the format described below<br>
 * <br>
 * <b>Benchmarks</b>: Generate as many tuples as possible in inline mode<br>
 * HashMap<String, String>: 8 million/sec with no classification; 1.8 million tuples/sec with classification<br>
 * HashMap<Sring, ArrayList<Integer>>: 8 million/sec with no classification; 3.5 million tuples/sec with classification<br>
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
 * <b>seed_end</b>Has to be an integer<br>
 * <b>key</b>If provided has to be in format "key1,key1start,key1end;key2, key2start, key2end; ..."
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * With key: Benchmarked at over 1 million tuples/second in local/in-line mode<br>
 * Without key: Benchmarked at over 4 million tuples/second in local/in-line mode<br>
 * </p>
 * @displayName Seed Event Generator
 * @category Testbench
 * @tags generate
 * @since 0.3.2
 */
public class SeedEventGenerator extends BaseOperator implements InputOperator
{
  @SuppressWarnings("rawtypes")
  /**
   * This output port emits generated tuples as a HashMap&lt;String, ArrayList&lt;KeyValPair&gt;&gt;.
   * The key in the map is a seed integer in the form of a string.
   * The value in the map is a list of key value pairs.
   * Each of these key value pairs is comprised of a specified string key,
   * and a randomly generated integer which lies within a specified range.
   */
  public final transient DefaultOutputPort<HashMap<String, ArrayList<KeyValPair>>> keyvalpair_list = new DefaultOutputPort<HashMap<String, ArrayList<KeyValPair>>>();
  /**
   * This output port emits generator tuples as a HashMap&lt;String, ArrayList&lt;Integer&gt;&gt;&gt;.
   * The key in the map is a seed integer in the form of a string.
   * The value in the map is a list of integers.
   * Each integer is randomly generated and lies within a specified range.
   */
  public final transient DefaultOutputPort<HashMap<String, ArrayList<Integer>>> val_list = new DefaultOutputPort<HashMap<String, ArrayList<Integer>>>();
  /**
   * This output port emits generator tuples as a HashMap&lt;String, String&gt;&gt;.
   * The key in the map is a seed integer in the form of a string.
   * The value in the map is a list of integers and their corresponding keys (in string form) that are randomly generated and lies within a specified range.
   */
  public final transient DefaultOutputPort<HashMap<String, String>> string_data = new DefaultOutputPort<HashMap<String, String>>();
  /**
   * This output port emits generator tuples as a HashMap&lt;String, String&gt;&gt;.
   * The key in the map is a seed integer in the form of a string.
   * The value in the map is a list of integers (in string form) that are randomly generated and lies within a specified range.
   */
  public final transient DefaultOutputPort<HashMap<String, String>> val_data = new DefaultOutputPort<HashMap<String, String>>();
  private static Logger LOG = LoggerFactory.getLogger(SeedEventGenerator.class);
  /**
   * Data for classification values
   */
  ArrayList<String> keys = null;
  ArrayList<Integer> keys_min = null;
  ArrayList<Integer> keys_range = null;
  int s_start = 0;
  int s_end = 99;
  private final Random random = new Random();

  public void setSeedStart(int i)
  {
    s_start = i;
  }

  public void setSeedEnd(int i)
  {
    s_end = i;
  }

  @Override
  public void emitTuples()
  {
    int lstart = s_start;
    int lend = s_end;

    if (lstart < lend) {
      for (int i = lstart; i < lend; i++) {
        emitTuple(i);
      }
    }
    else {
      for (int i = lstart; i > lend; i--) {
        emitTuple(i);
      }
    }
    // done generating data
    LOG.info("Finished generating data.");
    BaseOperator.shutdown();
  }

  /**
   *
   * Inserts a tuple for a given outbound key
   *
   * @param i
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void emitTuple(int i)
  {
    HashMap<String, String> stuple;
    HashMap<String, ArrayList<KeyValPair>> atuple;
    String key = Integer.toString(i);

    if (keys == null) {
      if (string_data.isConnected()) {
        stuple = new HashMap<String, String>(1);
        stuple.put(key, null);
        string_data.emit(stuple);
      }
      if (keyvalpair_list.isConnected()) {
        atuple = new HashMap<String, ArrayList<KeyValPair>>(1);
        atuple.put(key, null);
        keyvalpair_list.emit(atuple);
      }
      return;
    }

    ArrayList<KeyValPair> alist = null;
    ArrayList<Integer> vlist = null;
    String str = new String();
    String vstr = new String();
    boolean iskv = keyvalpair_list.isConnected();
    boolean isvl = val_list.isConnected();
    boolean issd = string_data.isConnected();
    boolean isvd = val_data.isConnected();

    int j = 0;
    for (String s: keys) {
      if (iskv) {
        if (alist == null) {
          alist = new ArrayList<KeyValPair>(keys.size());
        }
        alist.add(new KeyValPair<String, Integer>(s, new Integer(keys_min.get(j) + random.nextInt(keys_range.get(j)))));
      }
      if (isvl) {
        if (vlist == null) {
          vlist = new ArrayList<Integer>(keys.size());
        }
        vlist.add(new Integer(keys_min.get(j) + random.nextInt(keys_range.get(j))));
      }

      if (issd) {
        if (!str.isEmpty()) {
          str += ';';
        }
        str += s + ":" + Integer.toString(keys_min.get(j) + random.nextInt(keys_range.get(j)));
      }
      if (isvd) {
        if (!vstr.isEmpty()) {
          vstr += ';';
        }
        vstr += Integer.toString(keys_min.get(j) + random.nextInt(keys_range.get(j)));
      }
      j++;
    }

    if (iskv) {
      atuple = new HashMap<String, ArrayList<KeyValPair>>(1);
      atuple.put(key, alist);
      keyvalpair_list.emit(atuple);
    }

    if (isvl) {
      HashMap<String, ArrayList<Integer>> ituple = new HashMap<String, ArrayList<Integer>>(1);
      ituple.put(key, vlist);
      val_list.emit(ituple);
    }

    if (issd) {
      stuple = new HashMap<String, String>(1);
      stuple.put(key, str);
      string_data.emit(stuple);
    }

    if (isvd) {
      HashMap vtuple = new HashMap<String, String>(1);
      vtuple.put(key, vstr);
      val_data.emit(vtuple);
    }
  }

  /**
   *
   * Add a key data. By making a single call we ensure that all three Arrays are not corrupt and that the addition is atomic/one place
   *
   * @param key
   * @param low
   * @param high
   */
  public void addKeyData(String key, int low, int high)
  {
    if (keys == null) {
      keys = new ArrayList<String>();
      keys_min = new ArrayList<Integer>();
      keys_range = new ArrayList<Integer>();
    }

    keys.add(key);
    keys_min.add(low);
    keys_range.add(high - low + 1);
  }
}
