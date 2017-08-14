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
package org.apache.apex.malhar.lib.function;

import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The top level function interface <br>
 * The function is wrapped by {@link FunctionOperator} <br>
 * It takes input from input port of {@link FunctionOperator} ex. {@link FunctionOperator.MapFunctionOperator#input} <br>
 * And the output will be emitted using {@link FunctionOperator#tupleOutput} <br>
 * Anonymous function is not fully supported. It must be <b>stateless</b> should not be defined in any static context<br>
 * If anonymous function does not working, you can should use top level function class<br>
 * Top level function class should have public non-arg constructor
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public interface Function extends java.io.Serializable
{
  /**
   * If the {@link Function} implements this interface.
   * The state of the function will be checkpointed
   */
  public static interface Stateful
  {

  }

  /**
   * An interface defines a one input one output transformation
   * @param <I>
   * @param <O>
   */
  public static interface MapFunction<I, O> extends Function
  {
    O f(I input);
  }

  /**
   * A special map function to convert any pojo to key value pair datastructure
   * @param <T>
   * @param <K>
   * @param <V>
   */
  public static interface ToKeyValue<T, K, V> extends MapFunction<T, Tuple<KeyValPair<K, V>>>
  {

  }

  /**
   * An interface that defines flatmap transformation
   * @param <I>
   * @param <O>
   */
  public static interface FlatMapFunction<I, O> extends MapFunction<I, Iterable<O>>
  {
  }

  /**
   * An interface that defines filter transformation
   * @param <T>
   */
  public static interface FilterFunction<T> extends Function
  {
    boolean f(T input);
  }
}
