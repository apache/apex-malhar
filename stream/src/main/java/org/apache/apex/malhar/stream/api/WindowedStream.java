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
import java.util.Map;

import org.joda.time.Duration;

import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;


import com.datatorrent.lib.util.KeyValPair;

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
public interface WindowedStream<T> extends ApexStream<T>
{

  /**
   * Count of all tuples
   * @return new stream of Integer
   */
  <STREAM extends WindowedStream<Long>> STREAM count();

  /**
   * Count tuples by the key<br>
   * If the input is KeyedTuple it will get the key from getKey method from the tuple<br>
   * If not, use the tuple itself as a key
   * @return new stream of Map
   */
  <K, STREAM extends WindowedStream<Tuple<KeyValPair<K, Long>>>> STREAM countByKey(Function.MapFunction<T, Tuple<KeyValPair<K, Long>>> convertToKeyValue);

  /**
   *
   * Count tuples by the indexed key
   * @param key the index of the field in the tuple that are used as key
   * @return new stream of Map
   */
  <STREAM extends WindowedStream<Map<Object, Integer>>> STREAM countByKey(int key);


  /**
   *
   * Return top tuples by the selected key
   * @return new stream of Key and top N tuple of the key
   */
  <TUPLE, KEY, STREAM extends WindowedStream<Tuple<KeyValPair<KEY, List<TUPLE>>>>> STREAM topByKey(int N, Function.MapFunction<T, Tuple<KeyValPair<KEY, TUPLE>>> convertToKeyVal);

  /**
   *
   * Return top tuples of all tuples in the window
   * @return new stream of Map
   */
  <STREAM extends WindowedStream<T>> STREAM top(int N);

  <O, STREAM extends WindowedStream<O>> STREAM combineByKey();

  <O, STREAM extends WindowedStream<O>> STREAM combine();

  /**
   * Reduce transformation<br>
   * Add an operator to the DAG which merge tuple t1, t2 to new tuple
   * @param name operator name
   * @param reduce reduce function
   * @return new stream of same type
   */
  <STREAM extends WindowedStream<T>> STREAM reduce(String name, Function.ReduceFunction<T> reduce);

  /**
   * Fold transformation<br>
   * Add an operator to the DAG which merge tuple T to accumulated result tuple O
   * @param initialValue initial result value
   * @param fold fold function
   * @param <O> Result type
   * @return new stream of type O
   */
  <O, STREAM extends WindowedStream<O>> STREAM fold(O initialValue, Function.FoldFunction<T, O> fold);

  /**
   * Fold transformation<br>
   * Add an operator to the DAG which merge tuple T to accumulated result tuple O
   * @param name name of the operator
   * @param initialValue initial result value
   * @param fold fold function
   * @param <O> Result type
   * @return new stream of type O
   */
  <O, STREAM extends WindowedStream<O>> STREAM fold(String name, O initialValue, Function.FoldFunction<T, O> fold);


  /**
   * Fold transformation<br>
   * Add an operator to the DAG which merge tuple T to accumulated result tuple O
   * @param name name of the operator
   * @param fold fold function
   * @param <O> Result type
   * @return new stream of type O
   */
  <O, K, STREAM extends WindowedStream<KeyValPair<K, O>>> STREAM foldByKey(String name, Function.FoldFunction<T, KeyValPair<K, O>> fold);

  /**
   * Fold transformation<br>
   * Add an operator to the DAG which merge tuple T to accumulated result tuple O
   * @param fold fold function
   * @param <O> Result type
   * @return new stream of type O
   */
  <O, K, STREAM extends WindowedStream<KeyValPair<K, O>>> STREAM foldByKey(Function.FoldFunction<T, KeyValPair<K, O>> fold);


  /**
   * Reduce transformation<br>
   * Add an operator to the DAG which merge tuple t1, t2 to new tuple
   * @param reduce reduce function
   * @return new stream of same type
   */
  <STREAM extends WindowedStream<T>> STREAM reduce(Function.ReduceFunction<T> reduce);

  /**
   * Return tuples for each key for each window
   * @param <O>
   * @param <K>
   * @param <STREAM>
   * @return
   */
  <O, K, STREAM extends WindowedStream<KeyValPair<K, Iterable<O>>>> STREAM groupByKey(Function.MapFunction<T, KeyValPair<K, O>> convertToKeyVal);

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
