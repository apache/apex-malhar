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
package com.datatorrent.lib.algo;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.lib.util.KeyValPair;

import com.datatorrent.common.util.BaseOperator;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;

/**
 * This operator counts the number of unique values corresponding to a key within a window.&nbsp;
 * At the end of each window each key, its count of unique values, and its set of unique values is emitted.
 * <p>
 * Counts no. of unique values of a key within a window.<br>
 * Emits {@link InternalCountOutput} which contains the key, count of its unique values.<br>
 * When the operator is partitioned, the unifier uses the internal set of values to
 * compute the count of unique values again.<br>
 * <br>
 * Partitions: yes, uses {@link UniqueCountUnifier} to merge partitioned output.<br>
 * Stateful: no<br>
 * <br></br>
 * </p>
 *
 * @param <K>Type of Key objects</K>
 * @displayName Unique Values Per Key
 * @category Algorithmic
 * @tags count, key value
 * @since 0.3.5
 */

@Stateless
@OperatorAnnotation(partitionable = true)
public class UniqueValueCount<K> extends BaseOperator
{

  private final Map<K, Set<Object>> interimUniqueValues;

  /**
   * The input port that receives key value pairs.
   */
  public transient DefaultInputPort<KeyValPair<K, Object>> input = new DefaultInputPort<KeyValPair<K, Object>>()
  {

    @Override
    public void process(KeyValPair<K, Object> pair)
    {
      Set<Object> values = interimUniqueValues.get(pair.getKey());
      if (values == null) {
        values = Sets.newHashSet();
        interimUniqueValues.put(pair.getKey(), values);
      }
      values.add(pair.getValue());
    }
  };

  /**
   * The output port which emits key/unique value count pairs.
   */
  public transient DefaultOutputPort<KeyValPair<K, Integer>> output = new DefaultOutputPort<KeyValPair<K, Integer>>()
  {
    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Unifier<KeyValPair<K, Integer>> getUnifier()
    {
      return (Unifier) new UniqueCountUnifier<K>();
    }
  };

  /**
   * The output port which emits key and set containing unique values
   */
  public final transient DefaultOutputPort<KeyValPair<K, Set<Object>>> outputValues = new DefaultOutputPort<KeyValPair<K, Set<Object>>>()
  {
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Unifier<KeyValPair<K, Set<Object>>> getUnifier()
    {
      return (Unifier) new UniqueCountSetUnifier<K>();
    }
  };

  public UniqueValueCount()
  {
    this.interimUniqueValues = Maps.newHashMap();
  }

  @Override
  public void endWindow()
  {
    for (K key : interimUniqueValues.keySet()) {
      Set<Object> values = interimUniqueValues.get(key);
      if (output.isConnected()) {
        output.emit(new InternalCountOutput<>(key, values.size(), values));
      }
      if (outputValues.isConnected()) {
        outputValues.emit(new KeyValPair<>(key, values));
      }
    }
    interimUniqueValues.clear();
  }

  /**
   * State which contains a key, a set of values of that key, and a count of unique values of that key.<br></br>
   *
   * @param <K>Type of key objects</K>
   */
  public static class InternalCountOutput<K> extends KeyValPair<K, Integer>
  {

    private final Set<Object> interimUniqueValues;

    @SuppressWarnings("unused")
    protected InternalCountOutput()
    {
      this(null, null, null);
    }

    public InternalCountOutput(K k, Integer count, Set<Object> interimUniqueValues)
    {
      super(k, count);
      this.interimUniqueValues = interimUniqueValues;
    }

    public Set<Object> getInternalSet()
    {
      return interimUniqueValues;
    }
  }

  /**
   * Unifier for {@link UniqueValueCount} operator.<br>
   * It uses the internal set of values emitted by the operator and
   * emits {@link KeyValPair} of the key and a set of unique values matching key.<br></br>
   *
   * @param <K>Type of Key objects</K>
   */

  static class UniqueCountSetUnifier<K> implements Unifier<KeyValPair<K, Set<Object>>>
  {

    public final transient DefaultOutputPort<KeyValPair<K, Set<Object>>> output = new DefaultOutputPort<>();

    private final Map<K, Set<Object>> finalUniqueValues;

    public UniqueCountSetUnifier()
    {
      this.finalUniqueValues = Maps.newHashMap();
    }

    @Override
    public void process(KeyValPair<K, Set<Object>> tuple)
    {
      Set<Object> values = finalUniqueValues.get(tuple.getKey());
      if (values == null) {
        values = Sets.newHashSet();
        finalUniqueValues.put(tuple.getKey(), values);
      }
      values.addAll(tuple.getValue());
    }

    @Override
    public void beginWindow(long l)
    {
    }

    @Override
    public void endWindow()
    {
      for (Map.Entry<K, Set<Object>> entry : finalUniqueValues.entrySet()) {
        output.emit(new KeyValPair<>(entry.getKey(), entry.getValue()));
      }
      finalUniqueValues.clear();
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

  /**
   * Unifier for {@link UniqueValueCount} operator.<br>
   * It uses the internal set of values emitted by the operator and
   * emits {@link KeyValPair} of the key and its unique count.<br></br>
   *
   * @param <K>Type of Key objects</K>
   */
  static class UniqueCountUnifier<K> implements Unifier<InternalCountOutput<K>>
  {

    public final transient DefaultOutputPort<InternalCountOutput<K>> output = new DefaultOutputPort<>();

    private final Map<K, Set<Object>> finalUniqueValues;

    public UniqueCountUnifier()
    {
      this.finalUniqueValues = Maps.newHashMap();
    }

    @Override
    public void process(InternalCountOutput<K> tuple)
    {
      Set<Object> values = finalUniqueValues.get(tuple.getKey());
      if (values == null) {
        values = Sets.newHashSet();
        finalUniqueValues.put(tuple.getKey(), values);
      }
      values.addAll(tuple.interimUniqueValues);
    }

    @Override
    public void beginWindow(long l)
    {
    }

    @Override
    public void endWindow()
    {
      for (K key : finalUniqueValues.keySet()) {
        output.emit(new InternalCountOutput<>(key, finalUniqueValues.get(key).size(), finalUniqueValues.get(key)));
      }
      finalUniqueValues.clear();
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
