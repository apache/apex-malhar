/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseUniqueKeyCounter;
import com.datatorrent.lib.util.KeyHashValPair;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.HashMap;
import java.util.Map;

/**
 * This operator counts the number of times a key exists in a window.&nbsp;A map from keys to counts is emitted at the end of each window.
 * <p></p>
 * @displayName Count Key Appearances
 * @category algorithm
 * @tags algorithm, count, key value
 *
 * @since 1.0.2
 */
public class PartitionableUniqueCount<K> extends BaseUniqueKeyCounter<K>
{

  protected boolean cumulative = false;

  @InputPortFieldAnnotation(name = "data")
  public transient final DefaultInputPort<K> data = new DefaultInputPort<K>()
  {
    @Override
    public void process(K k)
    {
      processTuple(k);
    }
  };

  @InputPortFieldAnnotation(name = "data1", optional = true)
  public transient final DefaultInputPort<K> data1 = new DefaultInputPort<K>()
  {
    @Override
    public void process(K k)
    {
      processTuple(k);
    }
  };

  @OutputPortFieldAnnotation(name = "count")
  public final transient DefaultOutputPort<KeyHashValPair<K, Integer>> count
      = new DefaultOutputPort<KeyHashValPair<K, Integer>>()
  {
    @Override
    public Unifier<KeyHashValPair<K, Integer>> getUnifier()
    {
      return new UniqueCountUnifier<K>();
    }
  };

  @Override
  public void endWindow()
  {
    for (Map.Entry<K, MutableInt> e : map.entrySet()) {
      K key = e.getKey();
      count.emit(new KeyHashValPair<K, Integer>(key, e.getValue().toInteger()));
    }

    if (!cumulative) {
      map.clear();
    }
  }

  public void setCumulative(boolean c)
  {
    cumulative = c;
  }

  public boolean getCumulative()
  {
    return cumulative;
  }

  // Use of unifier is required only when, operator is partitioned with non sticky partition.
  public static class UniqueCountUnifier<K> implements Unifier<KeyHashValPair<K, Integer>>
  {

    public final transient DefaultOutputPort<KeyHashValPair<K, Integer>> mergedport =
        new DefaultOutputPort<KeyHashValPair<K, Integer>>();

    private Map<K, MutableInt> map = new HashMap<K, MutableInt>();

    @Override
    public void process(KeyHashValPair<K, Integer> tupple)
    {
      MutableInt count = map.get(tupple.getKey());
      if (count == null) {
        count = new MutableInt(0);
        map.put(tupple.getKey(), count);
      }
      count.add(tupple.getValue());
    }

    @Override
    public void beginWindow(long l)
    {

    }

    @Override
    public void endWindow()
    {
      for (Map.Entry<K, MutableInt> e : map.entrySet()) {
        K key = e.getKey();
        mergedport.emit(new KeyHashValPair<K, Integer>(key, e.getValue().toInteger()));
      }
      map.clear();
    }

    @Override
    public void setup(Context.OperatorContext operatorContext)
    {

    }

    @Override
    public void teardown()
    {

    }
  }
}
