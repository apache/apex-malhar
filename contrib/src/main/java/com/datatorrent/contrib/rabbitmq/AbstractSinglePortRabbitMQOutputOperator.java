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
package com.datatorrent.contrib.rabbitmq;

import com.datatorrent.api.DefaultInputPort;

/**
 * This is the base implementation of a single port RabbitMQ output operator.&nbsp;
 * Subclasses should implement the methods which convert tuples to RabbitMQ messages.
 * <p>
 * Ports:<br>
 * <b>Input</b>: Can have one input port<br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for AbstractSinglePortRabbitMQOutputOperator&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td>One tuple per key per window per port</td><td><b>10 thousand K,V pairs/s</td><td>Out-bound rate is the main determinant of performance. Operator can process about 10 thousand unique (k,v immutable pairs) tuples/sec as RabbitMQ DAG. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may differ</td></tr>
 * </table><br>
 * </p>
 * @displayName Abstract Single Port RabbitMQ Output
 * @category Messaging
 * @tags output operator
 *
 * @since 0.3.2
 */
public abstract class AbstractSinglePortRabbitMQOutputOperator<T> extends AbstractRabbitMQOutputOperator
{
  /**
   * Users need to provide conversion of Tuple to message
   * @param tuple
   */
  public abstract void processTuple(T tuple);

  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      if(!skipProcessingTuple)
      {
        processTuple(tuple); // This is an abstract call
      }
    }
  };
}
