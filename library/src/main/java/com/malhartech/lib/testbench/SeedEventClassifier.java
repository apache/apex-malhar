/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.OperatorConfiguration;
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
 *
 * @author amol
 */
public class SeedEventClassifier<T> extends BaseOperator
{
  public final transient DefaultInputPort<T> data1 = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T tuple)
    {
      emitTuple(key1, tuple);
    }
  };
  public final transient DefaultInputPort<T> data2 = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T tuple)
    {
      emitTuple(key2, tuple);
    }
  };
  public final transient DefaultOutputPort<String> string_data = new DefaultOutputPort<String>(this);
  public final transient DefaultOutputPort<HashMap<String, HashMap<String, T>>> hash_data = new DefaultOutputPort<HashMap<String, HashMap<String, T>>>(this);

  public void emitTuple(String key, T tuple)
  {
    if (string_data.isConnected()) {
      String str = Integer.toString(seed);
      str += ":";
      str += key;
      str += ",";
      str += tuple.toString();
      string_data.emit(str);
    }
    if (hash_data.isConnected()) {
      HashMap<String, HashMap<String, T>> hdata = new HashMap<String, HashMap<String, T>>(1);
      HashMap<String, T> val = new HashMap<String, T>(1);
      val.put(key, tuple);
      hdata.put(Integer.toString(seed), val);
      hash_data.emit(hdata);
    }
    seed++;
    if (seed == s_end) {
      seed = s_start;
    }
  }
  /**
   * Data for classification values
   */
  HashMap<String, Object> keys = new HashMap<String, Object>();
  String key1 = new String();
  String key2 = new String();
  int s_start = 0;
  int s_end = 99;
  int seed = 0;

  public void setSeedstart(int i)
  {
    s_start = i;
  }

  public void setSeedend(int i)
  {
    s_end = i;
  }

  public void setKey1(String i)
  {
    key1 = i;
  }

  public void setKey2(String i)
  {
    key2 = i;
  }

  @Override
  public void setup(OperatorConfiguration config)
  {
    if (s_start > s_end) {
      int temp = s_end;
      s_end = s_start;
      s_start = temp;
    }
    seed = s_start;
  }
}
