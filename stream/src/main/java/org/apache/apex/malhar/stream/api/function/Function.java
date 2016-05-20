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
package org.apache.apex.malhar.stream.api.function;

/**
 * The top level function interface
 *
 * @since 3.4.0
 */
public interface Function
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
   * An interface defines a reduce transformation
   * @param <T>
   */
  public static interface ReduceFunction<T> extends Function
  {
    T reduce(T t1, T t2);
  }

  /**
   * An interface that defines a fold transformation
   * @param <I>
   * @param <O>
   */
  public static interface FoldFunction<I, O> extends Function
  {
    O fold(I input, O output);
  }

  /**
   * An interface that defines flatmap transforation
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
  public static interface FilterFunction<T> extends MapFunction<T, Boolean>
  {
  }
}
