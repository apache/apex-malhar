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
package com.datatorrent.contrib.zmq;

import com.datatorrent.api.DefaultInputPort;

/**
 * This is a ZeroMQ output adapter, with a single input port.&nbsp;
 * This operator will behave like a publisher that replies to requests.
 * <p>
 * <br>
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
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for AbstractSinglePortZeroMQOutputOperator&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td>One tuple per key per window per port</td><td><b>400 thousand K,V pairs/s</td><td>Out-bound rate is the main determinant of performance. Operator can process about 400 thousand unique (k,v immutable pairs) tuples/sec as ZeroMQ DAG. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may differ</td></tr>
 * </table><br>
 * <br>
 * </p>
 * @displayName Abstract Single Port ZeroMQ Output
 * @category messaging
 * @tags output
 * @since 0.3.2
 */
public abstract class AbstractSinglePortZeroMQOutputOperator<T> extends AbstractBaseZeroMQOutputOperator
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
      processTuple(tuple); // This is an abstract call
    }
  };
}
