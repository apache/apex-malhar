/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.api.InputOperator;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.OneKeyValPair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.AsyncInputOperator;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.OneKeyValPair;

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
 *
 * @author amol
 */
public class SeedEventGenerator extends BaseOperator implements InputOperator
{
  public final transient DefaultOutputPort<HashMap<String, ArrayList<OneKeyValPair>>> keyvalpair_list = new DefaultOutputPort<HashMap<String, ArrayList<OneKeyValPair>>>(this);
  public final transient DefaultOutputPort<HashMap<String, ArrayList<Integer>>> val_list = new DefaultOutputPort<HashMap<String, ArrayList<Integer>>>(this);
  public final transient DefaultOutputPort<HashMap<String, String>> string_data = new DefaultOutputPort<HashMap<String, String>>(this);
  public final transient DefaultOutputPort<HashMap<String, String>> val_data = new DefaultOutputPort<HashMap<String, String>>(this);
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

  public void setSeedstart(int i)
  {
    s_start = i;
  }

  public void setSeedend(int i)
  {
    s_end = i;
  }

  @Override
  public void emitTuples(long windowId)
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
  }

  /**
   *
   * Inserts a tuple for a given outbound key
   *
   * @param tuples bag of tuples
   * @param key the key
   */
  public void emitTuple(int i)
  {
    HashMap<String, String> stuple = null;
    HashMap<String, ArrayList<OneKeyValPair>> atuple = null;
    String key = Integer.toString(i);

    if (keys == null) {
      if (string_data.isConnected()) {
        stuple.put(key, null);
        string_data.emit(stuple);
      }
      if (keyvalpair_list.isConnected()) {
        atuple.put(key, null);
        keyvalpair_list.emit(atuple);
      }
      return;
    }

    ArrayList<OneKeyValPair> alist = null;
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
          alist = new ArrayList<OneKeyValPair>(keys.size());
        }
        alist.add(new OneKeyValPair<String, Integer>(s, new Integer(keys_min.get(j) + random.nextInt(keys_range.get(j)))));
      }
      if (isvl) {
        if (alist == null) {
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
      atuple = new HashMap<String, ArrayList<OneKeyValPair>>(1);
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

  @Override
  public void replayTuples(long windowId)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
