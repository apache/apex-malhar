/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.testbench;

import java.util.HashMap;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * This operator receives data on two input ports (data1, and data2).&nbsp;
 * Each incoming tuple is given a seed value
 * and a key depending on which input port the tuple came from.&nbsp;
 * Modified tuples are then emitted as strings on the string_data output port
 * and as maps on the hash_data output port.
 * <p>
 * <br>
 * Examples of getting seed distributions include<br>
 * Changes in mobile co-ordinates of a phone<br>
 * Random changes on motion of an object<br>
 * <br>
 * The seed is created from the values of properties <b>seed_start</b>, and <b>seed_end</b>
 * <br>
 * <b>Default schema</b>:<br>
 * Schema for port <b>data</b>: The default schema is HashMap<String, ArrayList<valueData>>, where valueData is class{String, Integer}<br>
 * <b>String schema</b>: The string is "key;valkey1:value1;valkey2:value2;..."<br>
 * <b>HashMap schema</b>: Key is String, and Value is a ArrrayList<String, Number><br>
 * The value in both the schemas is an integer (for choice of strings, these are enum values)
 * <br>
 * <b>Port Interface</b><br>
 * <b>data1</b>: Expects tuples of type <T><br>
 * <b>data2</b>: Expects tuples of type <T><br>
 * <b>string_data</b>: Emits new classified seed of schema String<br>
 * <b>hash_data</b>: Emits new classified seed of schema HashMap<String, HashMap<String, T>><br>
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
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * String: Benchmarked at over 13 million tuples/second in local/in-line mode<br>
 * Integer: Benchmarked at over 7 million tuples/second in local/in-line mode<br>
 * </p>
 * @displayName Seed Event Classifier
 * @category Test Bench
 * @tags generate
 * @since 0.3.2
 */
public class SeedEventClassifier<T> extends BaseOperator
{
  /**
   * An input port which receives incoming tuples.
   */
  public final transient DefaultInputPort<T> data1 = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      emitTuple(key1, tuple);
    }
  };

  /**
   * An output port which receives incoming tuples.
   */
  public final transient DefaultInputPort<T> data2 = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      emitTuple(key2, tuple);
    }
  };

  /**
   * An output port which emits modified tuples as a string.
   */
  public final transient DefaultOutputPort<String> string_data = new DefaultOutputPort<String>();
  /**
   * An output port which emits modified tuples as a hashmap.
   */
  public final transient DefaultOutputPort<HashMap<String, HashMap<String, T>>> hash_data = new DefaultOutputPort<HashMap<String, HashMap<String, T>>>();

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
  @NotNull
  String key1 = new String();
  @NotNull
  String key2 = new String();
  int s_start = 0;
  int s_end = 99;
  int seed = 0;

  @NotNull
  public String getKey1()
  {
    return key1;
  }

  @NotNull
  public String getKey2()
  {
    return key2;
  }

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
  public void setup(OperatorContext context)
  {
    if (s_start > s_end) {
      int temp = s_end;
      s_end = s_start;
      s_start = temp;
    }
    seed = s_start;
  }
}
