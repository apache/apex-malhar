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
package org.apache.apex.malhar.stream.api.impl;

import java.util.List;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;

import org.apache.apex.malhar.lib.window.accumulation.FoldFn;
import org.apache.apex.malhar.lib.window.accumulation.ReduceFn;
import org.apache.apex.malhar.lib.window.accumulation.SumLong;
import org.apache.apex.malhar.lib.window.accumulation.TopN;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;

import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.Option;
import org.apache.apex.malhar.stream.api.WindowedStream;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Default windowed stream implementation for WindowedStream interface.
 * It adds more windowed transform for Stream interface
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public class ApexWindowedStreamImpl<T> extends ApexStreamImpl<T> implements WindowedStream<T>
{

  protected WindowOption windowOption;

  protected TriggerOption triggerOption;

  protected Duration allowedLateness;

  private static class ConvertFn<T> implements Function.MapFunction<T, Tuple<T>>
  {

    @Override
    public Tuple<T> f(T input)
    {
      if (input instanceof Tuple.TimestampedTuple) {
        return (Tuple.TimestampedTuple)input;
      } else {
        return new Tuple.TimestampedTuple<>(System.currentTimeMillis(), input);
      }
    }
  }


  public ApexWindowedStreamImpl()
  {
  }

  @Override
  public <STREAM extends WindowedStream<Tuple.WindowedTuple<Long>>> STREAM count(Option... opts)
  {
    Function.MapFunction<T, Tuple<Long>> kVMap = new Function.MapFunction<T, Tuple<Long>>()
    {
      @Override
      public Tuple<Long> f(T input)
      {
        if (input instanceof Tuple.TimestampedTuple) {
          return new Tuple.TimestampedTuple<>(((Tuple.TimestampedTuple)input).getTimestamp(), 1L);
        } else {
          return new Tuple.TimestampedTuple<>(System.currentTimeMillis(), 1L);
        }
      }
    };

    WindowedStream<Tuple<Long>> innerstream = map(kVMap);
    WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = createWindowedOperator(new SumLong());
    return innerstream.addOperator(windowedOperator, windowedOperator.input, windowedOperator.output, opts);
  }

  @Override
  public <K, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, Long>>>> STREAM countByKey(Function.ToKeyValue<T, K, Long> convertToKeyValue, Option... opts)
  {
    WindowedStream<Tuple<KeyValPair<K, Long>>> kvstream = map(convertToKeyValue);
    KeyedWindowedOperatorImpl<K, Long, MutableLong, Long> keyedWindowedOperator = createKeyedWindowedOperator(new SumLong());
    return kvstream.addOperator(keyedWindowedOperator, keyedWindowedOperator.input, keyedWindowedOperator.output, opts);
  }

  @Override
  public <K, V, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, List<V>>>>> STREAM topByKey(int N, Function.ToKeyValue<T, K, V> convertToKeyVal, Option... opts)
  {
    TopN<V> top = new TopN<>();
    top.setN(N);
    WindowedStream<Tuple<KeyValPair<K, V>>> kvstream = map(convertToKeyVal);
    KeyedWindowedOperatorImpl<K, V, List<V>, List<V>> keyedWindowedOperator = createKeyedWindowedOperator(top);
    return kvstream.addOperator(keyedWindowedOperator, keyedWindowedOperator.input, keyedWindowedOperator.output, opts);
  }

  @Override
  public <STREAM extends WindowedStream<Tuple.WindowedTuple<List<T>>>> STREAM top(int N, Option... opts)
  {

    TopN<T> top = new TopN<>();
    top.setN(N);
    WindowedStream<Tuple<T>> innerstream = map(new ConvertFn<T>());
    WindowedOperatorImpl<T, List<T>, List<T>> windowedOperator = createWindowedOperator(top);
    return innerstream.addOperator(windowedOperator, windowedOperator.input, windowedOperator.output, opts);
  }


  @Override
  public <K, V, O, ACCU, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, O>>>> STREAM accumulateByKey(Accumulation<V, ACCU, O> accumulation,
      Function.ToKeyValue<T, K, V> convertToKeyVal, Option... opts)
  {
    WindowedStream<Tuple<KeyValPair<K, V>>> kvstream = map(convertToKeyVal);
    KeyedWindowedOperatorImpl<K, V, ACCU, O> keyedWindowedOperator = createKeyedWindowedOperator(accumulation);
    return kvstream.addOperator(keyedWindowedOperator, keyedWindowedOperator.input, keyedWindowedOperator.output, opts);
  }


  @Override
  public <O, ACCU, STREAM extends WindowedStream<Tuple.WindowedTuple<O>>> STREAM accumulate(Accumulation<T, ACCU, O> accumulation, Option... opts)
  {
    WindowedStream<Tuple<T>> innerstream = map(new ConvertFn<T>());
    WindowedOperatorImpl<T, ACCU, O> windowedOperator = createWindowedOperator(accumulation);
    return innerstream.addOperator(windowedOperator, windowedOperator.input, windowedOperator.output, opts);
  }


  @Override
  public <STREAM extends WindowedStream<Tuple.WindowedTuple<T>>> STREAM reduce(ReduceFn<T> reduce, Option... opts)
  {
    WindowedStream<Tuple<T>> innerstream = map(new ConvertFn<T>());
    WindowedOperatorImpl<T, T, T> windowedOperator = createWindowedOperator(reduce);
    return innerstream.addOperator(windowedOperator, windowedOperator.input, windowedOperator.output, opts);
  }

  @Override
  public <K, V, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, V>>>> STREAM reduceByKey(ReduceFn<V> reduce, Function.ToKeyValue<T, K, V> convertToKeyVal, Option... opts)
  {
    WindowedStream<Tuple<KeyValPair<K, V>>> kvstream = map(convertToKeyVal);
    KeyedWindowedOperatorImpl<K, V, V, V> keyedWindowedOperator = createKeyedWindowedOperator(reduce);
    return kvstream.addOperator(keyedWindowedOperator, keyedWindowedOperator.input, keyedWindowedOperator.output, opts);
  }

  @Override
  public <O, STREAM extends WindowedStream<Tuple.WindowedTuple<O>>> STREAM fold(FoldFn<T, O> fold, Option... opts)
  {
    WindowedStream<Tuple<T>> innerstream = map(new ConvertFn<T>());
    WindowedOperatorImpl<T, O, O> windowedOperator = createWindowedOperator(fold);
    return innerstream.addOperator(windowedOperator, windowedOperator.input, windowedOperator.output, opts);
  }

  @Override
  public <K, V, O, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, O>>>> STREAM foldByKey(FoldFn<V, O> fold, Function.ToKeyValue<T, K, V> convertToKeyVal, Option... opts)
  {
    WindowedStream<Tuple<KeyValPair<K, V>>> kvstream = map(convertToKeyVal);
    KeyedWindowedOperatorImpl<K, V, O, O> keyedWindowedOperator = createKeyedWindowedOperator(fold);
    return kvstream.addOperator(keyedWindowedOperator, keyedWindowedOperator.input, keyedWindowedOperator.output, opts);

  }

  @Override
  public <O, K, STREAM extends WindowedStream<KeyValPair<K, Iterable<O>>>> STREAM groupByKey(Function.ToKeyValue<T, K, O> convertToKeyVal, Option... opts)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <STREAM extends WindowedStream<Iterable<T>>> STREAM group()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <STREAM extends WindowedStream<T>> STREAM resetTrigger(TriggerOption option)
  {
    triggerOption = option;
    return (STREAM)this;
  }

  @Override
  public <STREAM extends WindowedStream<T>> STREAM resetAllowedLateness(Duration allowedLateness)
  {
    this.allowedLateness = allowedLateness;
    return (STREAM)this;
  }

  @Override
  protected <O> ApexStream<O> newStream(DagMeta graph, Brick<O> newBrick)
  {
    ApexWindowedStreamImpl<O> newstream = new ApexWindowedStreamImpl<>();
    newstream.graph = graph;
    newstream.lastBrick = newBrick;
    newstream.windowOption = this.windowOption;
    newstream.triggerOption = this.triggerOption;
    newstream.allowedLateness = this.allowedLateness;
    return newstream;
  }

  /**
   * Create the windowed operator for windowed transformation
   * @param accumulationFn
   * @param <IN>
   * @param <ACCU>
   * @param <OUT>
   * @return
   */
  private <IN, ACCU, OUT> WindowedOperatorImpl<IN, ACCU, OUT> createWindowedOperator(Accumulation<? super IN, ACCU, OUT> accumulationFn)
  {
    WindowedOperatorImpl<IN, ACCU, OUT> windowedOperator = new WindowedOperatorImpl<>();
    //TODO use other default setting in the future
    windowedOperator.setDataStorage(new InMemoryWindowedStorage<ACCU>());
    windowedOperator.setRetractionStorage(new InMemoryWindowedStorage<OUT>());
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    if (windowOption != null) {
      windowedOperator.setWindowOption(windowOption);
    }
    if (triggerOption != null) {
      windowedOperator.setTriggerOption(triggerOption);
    }
    if (allowedLateness != null) {
      windowedOperator.setAllowedLateness(allowedLateness);
    }
    windowedOperator.setAccumulation(accumulationFn);
    return windowedOperator;
  }

  private <K, V, ACCU, OUT> KeyedWindowedOperatorImpl<K, V, ACCU, OUT> createKeyedWindowedOperator(Accumulation<? super V, ACCU, OUT> accumulationFn)
  {
    KeyedWindowedOperatorImpl<K, V, ACCU, OUT> keyedWindowedOperator = new KeyedWindowedOperatorImpl<>();

    //TODO use other default setting in the future
    keyedWindowedOperator.setDataStorage(new InMemoryWindowedKeyedStorage<K, ACCU>());
    keyedWindowedOperator.setRetractionStorage(new InMemoryWindowedKeyedStorage<K, OUT>());
    keyedWindowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    if (windowOption != null) {
      keyedWindowedOperator.setWindowOption(windowOption);
    }
    if (triggerOption != null) {
      keyedWindowedOperator.setTriggerOption(triggerOption);
    }
    if (allowedLateness != null) {
      keyedWindowedOperator.setAllowedLateness(allowedLateness);
    }

    keyedWindowedOperator.setAccumulation(accumulationFn);
    return keyedWindowedOperator;
  }

}
