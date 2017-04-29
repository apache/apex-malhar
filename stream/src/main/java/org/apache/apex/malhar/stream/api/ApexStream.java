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

import java.util.concurrent.Callable;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;

/**
 * The stream interface to build a DAG
 * @param <T>
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public interface ApexStream<T>
{
  /**
   * Simple map transformation<br>
   * Add an operator to the DAG which convert tuple T to tuple O
   * @param mapFunction map function
   * @param <O> Type of the output
   * @return new stream of type O
   */
  <O, STREAM extends ApexStream<O>> STREAM map(Function.MapFunction<T, O> mapFunction, Option... opts);

  /**
   * Flat map transformation
   * Add an operator to the DAG which convert tuple T to a collection of tuple O
   * @param flatten flat map
   * @param <O> Type of the output
   * @return new stream of type O
   */
  <O, STREAM extends ApexStream<O>> STREAM flatMap(Function.FlatMapFunction<T, O> flatten, Option... opts);

  /**
   * Filter transformation<br>
   * Add an operator to the DAG which filter out tuple T that cannot satisfy the FilterFunction
   * @param filter filter function
   * @return new stream of same type
   */
  <STREAM extends ApexStream<T>> STREAM filter(Function.FilterFunction<T> filter, Option... opts);

  /**
   * Extend the dag by adding one operator<br>
   * @param op Operator added to the stream
   * @param inputPort InputPort of the operator that is connected to last exposed OutputPort in the stream
   * @param outputPort OutputPort of the operator will be connected to next operator
   * @param <O> type of the output
   * @return new stream of type O
   */
  <O, STREAM extends ApexStream<O>> STREAM addOperator(Operator op, Operator.InputPort<T> inputPort,  Operator.OutputPort<O> outputPort, Option... opts);

  /**
   * Extend the dag by adding one end operator<br>
   * @param op Operator added to the stream
   * @param inputPort InputPort of the operator that is connected to last exposed OutputPort in the stream
   * @param <O> type of the output
   * @return new stream of type O
   */
  <O, STREAM extends ApexStream<O>> STREAM endWith(Operator op, Operator.InputPort<T> inputPort, Option... opts);

  /**
   * Extend the dag by adding one {@see CompositeStreamTransform}
   * @param compositeStreamTransform Composite Streams and Transforms
   * @return new stream of type O
   */
  <O, INSTREAM extends ApexStream<T>, OUTSTREAM extends ApexStream<O>> OUTSTREAM addCompositeStreams(CompositeStreamTransform<INSTREAM, OUTSTREAM> compositeStreamTransform);

  /**
   * Union multiple stream into one
   * @param others  other streams
   * @return new stream of same type
   */
  <STREAM extends ApexStream<T>> STREAM union(ApexStream<T>... others);

  /**
   * Add a stdout console output operator
   * @return stream itself
   */
  <STREAM extends ApexStream<T>> STREAM print(Option... opts);

  /**
   * Add a stdout console output operator
   * @return stream itself
   */
  <STREAM extends ApexStream<T>> STREAM print();

  /**
   * Add a stderr console output operator
   * @return stream itself
   */
  <STREAM extends ApexStream<T>> STREAM printErr();

  /**
   * Set the attribute value<br>
   * If it is {@link DAGContext DAG attribute}, it will be applied to the whole DAG <br>
   * If it is {@link OperatorContext Operator attribute}, it will be applied to last connected operator<br>
   * If it is {@link PortContext InputPort attribute}, it will be applied to the input port of the last connected stream<br>
   * If it is {@link PortContext OutputPort attribute}, it will be applied to the output port of the last connected stream<br>
   * If it is both {@link PortContext InputPort&OutputPort attribute}, it will be applied to last connected stream
   * @param attribute {@see Attribute}
   * @param value value of the attribute
   * @return stream itself
   */
  <STREAM extends ApexStream<T>> STREAM with(Attribute attribute, Object value);

  /**
   * Set attributes at the DAG level
   * @param attribute {@see Attribute}
   * @param value value of the attribute
   * @return stream itself
   */
  <STREAM extends ApexStream<T>> STREAM setGlobalAttribute(Attribute attribute, Object value);

  /**
   * Set the locality
   * @param locality {@see DAG.Locality}
   * @return stream itself
   */
  <STREAM extends ApexStream<T>> STREAM with(DAG.Locality locality);

  /**
   * Set the property value of the last connected operator
   * @param propName property name
   * @param value value of the property
   * @return stream itself
   */
  <STREAM extends ApexStream<T>> STREAM with(String propName, Object value);


  /**
   * Create dag from stream
   * @return {@see DAG}
   */
  DAG createDag();

  /**
   * Populate existing dag
   * @param dag {@see DAG}
   */
  void populateDag(DAG dag);


  /**
   * Run the stream application in local mode
   * In Async mode, the method would return immediately and the dag would run for "duration" milliseconds
   * In Sync mode, the method would block "duration" milliseconds to run the dag
   * If duration is negative the dag would run forever until being killed
   * @param async true if run in Async mode
   *              false if run in sync mode
   */
  void runEmbedded(boolean async, long duration, Callable<Boolean> exitCondition);


  /**
   * Submit the application to cluster
   */
  void run();

  /**
   * Chunk tuples into Windows
   * Window Transform are defined in {@see WindowedStream}
   * @param windowOption
   * @return
   */
  WindowedStream<T> window(WindowOption windowOption);

  /**
   * Chunk tuple into windows with window option and trigger option
   * @param windowOption
   * @param triggerOption
   * @return
   */
  WindowedStream<T> window(WindowOption windowOption, TriggerOption triggerOption);

  /**
   *
   * Chunk tuple into windows with window option and trigger option and allowed lateness
   * @param windowOption
   * @param triggerOption
   * @param allowLateness
   * @return
   */
  WindowedStream<T> window(WindowOption windowOption, TriggerOption triggerOption, Duration allowLateness);

}
