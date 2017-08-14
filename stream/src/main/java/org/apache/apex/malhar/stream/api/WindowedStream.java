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
package org.apache.apex.malhar.stream.api;

import java.util.List;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.accumulation.FoldFn;
import org.apache.apex.malhar.lib.window.accumulation.ReduceFn;
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * <p>
 * A stream with windowed transformation
 * </p>
 * <p>
 * <B>Transformation types:</B>
 * <ul>
 * <li>Combine</li>
 * <li>Group</li>
 * <li>Keyed Combine</li>
 * <li>Keyed Group</li>
 * <li>Join</li>
 * <li>CoGroup</li>
 * </ul>
 * </p>
 * <p>
 * <B>Features supported with windowed transformation </B>
 * <ul>
 * <li>Watermark - Ingestion time watermark / logical tuple watermark</li>
 * <li>Early Triggers - How frequent to emit real-time partial result</li>
 * <li>Late Triggers - When to emit updated result with tuple comes after watermark</li>
 * <li>Customizable Trigger Behaviour - What to do when fires a trigger</li>
 * <li>Spool window state -  In-Memory window state can be spooled to disk if it is full</li>
 * <li>3 different accumulation models: ignore, accumulation, accumulation + delta</li>
 * <li>Window support: Non-Mergeable window(fix window, sliding window), Mergeable window(session window) base on 3 different tuple time</li>
 * <li>Different tuple time support: event time, system time, ingestion time</li>
 * </ul>
 * </p>
 *
 * @param <T> Output tuple type
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public interface WindowedStream<T> extends ApexStream<T>
{

  /**
   * Count of all tuples
   * @return new stream of Integer
   */
  <STREAM extends WindowedStream<Tuple.WindowedTuple<Long>>> STREAM count(Option... opts);

  /**
   * Count tuples by the key<br>
   * @param convertToKeyValue The function convert plain tuple to k,v pair
   * @return new stream of Key Value Pair
   */
  <K, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, Long>>>> STREAM countByKey(Function.ToKeyValue<T, K, Long> convertToKeyValue, Option... opts);

  /**
   * Return top N tuples by the selected key
   * @param N how many tuples you want to keep
   * @param convertToKeyVal The function convert plain tuple to k,v pair
   * @return new stream of Key and top N tuple of the key
   */
  <K, V, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, List<V>>>>> STREAM topByKey(int N, Function.ToKeyValue<T, K, V> convertToKeyVal, Option... opts);

  /**
   * Return top N tuples of all tuples in the window
   * @param N
   * @return new stream of topN
   */
  <STREAM extends WindowedStream<Tuple.WindowedTuple<List<T>>>> STREAM top(int N, Option... opts);

  /**
   * Add {@link KeyedWindowedOperatorImpl} with specified {@link Accumulation} <br>
   * Accumulate tuples by some key within the window definition in this stream
   * Also give a name to the accumulation
   * @param accumulation Accumulation function you want to do
   * @param convertToKeyVal The function convert plain tuple to k,v pair
   * @param <K> The type of the key used to group tuples
   * @param <V> The type of value you want to do accumulation on
   * @param <O> The output type for each given key that you want to accumulate the value to
   * @param <ACCU> The type of accumulation you want to keep (it can be in memory or on disk)
   * @param <STREAM> return type
   * @return
   */
  <K, V, O, ACCU, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, O>>>> STREAM accumulateByKey(Accumulation<V, ACCU, O> accumulation,
      Function.ToKeyValue<T, K, V> convertToKeyVal, Option... opts);

  /**
   * Add {@link WindowedOperatorImpl} with specified {@link Accumulation} <br>
   * Accumulate tuples by some key within the window definition in this stream
   * Also give a name to the accumulation
   * @param accumulation Accumulation function you want to do
   * @param <O> The output type that you want to accumulate the value to
   * @param <ACCU> The type of accumulation you want to keep (it can be in memory or on disk)
   * @param <STREAM> return type
   * @return
   */
  <O, ACCU, STREAM extends WindowedStream<Tuple.WindowedTuple<O>>> STREAM accumulate(Accumulation<T, ACCU, O> accumulation, Option... opts);

  /**
   * Add {@link WindowedOperatorImpl} with specified {@link ReduceFn} <br>
   * Do reduce transformation<br>
   * @param reduce reduce function
   * @param <STREAM> return type
   * @return new stream of same type
   */
  <STREAM extends WindowedStream<Tuple.WindowedTuple<T>>> STREAM reduce(ReduceFn<T> reduce, Option... opts);

  /**
   * Add {@link KeyedWindowedOperatorImpl} with specified {@link ReduceFn} <br>
   * Reduce transformation by selected key <br>
   * Add an operator to the DAG which merge tuple t1, t2 to new tuple by key
   * @param reduce reduce function
   * @param convertToKeyVal The function convert plain tuple to k,v pair
   * @param <K> The type of key you want to group tuples by
   * @param <V> The type of value extract from tuple T
   * @param <STREAM> return type
   * @return new stream of key value pair
   */
  <K, V, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, V>>>> STREAM reduceByKey(ReduceFn<V> reduce, Function.ToKeyValue<T, K, V> convertToKeyVal, Option... opts);


  /**
   * Add {@link WindowedOperatorImpl} with specified {@link FoldFn} <br>
   * Fold transformation <br>
   * @param fold fold function
   * @param <O> output type of fold function
   * @param <STREAM> return type
   * @return
   */
  <O, STREAM extends WindowedStream<Tuple.WindowedTuple<O>>> STREAM fold(FoldFn<T, O> fold, Option... opts);

  /**
   * Add {@link KeyedWindowedOperatorImpl} with specified {@link FoldFn} <br>
   * Fold transformation by key <br>
   * @param fold fold function
   * @param <O> Result type
   * @return new stream of type O
   */
  <K, V, O, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, O>>>> STREAM foldByKey(FoldFn<V, O> fold, Function.ToKeyValue<T, K, V> convertToKeyVal, Option... opts);


  /**
   * Return tuples for each key for each window
   * @param <O>
   * @param <K>
   * @param <STREAM>
   * @return
   */
  <O, K, STREAM extends WindowedStream<KeyValPair<K, Iterable<O>>>> STREAM groupByKey(Function.ToKeyValue<T, K, O> convertToKeyVal, Option... opts);

  /**
   * Return tuples for each window
   * @param <STREAM>
   * @return
   */
  <STREAM extends WindowedStream<Iterable<T>>> STREAM group();

  /**
   * Reset the trigger settings for next transforms
   * @param triggerOption
   * @param <STREAM>
   */
  <STREAM extends WindowedStream<T>> STREAM resetTrigger(TriggerOption triggerOption);

  /**
   * Reset the allowedLateness settings for next transforms
   * @param allowedLateness
   * @param <STREAM>
   */
  <STREAM extends WindowedStream<T>> STREAM resetAllowedLateness(Duration allowedLateness);

}
