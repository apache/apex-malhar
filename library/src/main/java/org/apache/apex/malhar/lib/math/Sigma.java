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
package org.apache.apex.malhar.lib.math;

import java.util.Collection;

import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * Adds incoming tuple to the state and emits the result of each addition on the respective ports.
 * <p>
 * The addition would go on forever.Result is emitted on four different data type ports:floatResult,integerResult,longResult,doubleResult.
 * Input tuple object has to be an implementation of the interface Collection.Tuples are emitted on the output ports only if they are connected.
 * This is done to avoid the cost of calling the functions when some ports are not connected.
 * This is a stateful pass through operator<br>
 * <b>Partitions : </b>, no will yield wrong results, no unifier on output port.
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Collection&lt;T extends Number&lt;<br>
 * <b>doubleResult</b>: emits Double<br>
 * <b>floatResult</b>: emits Float<br>
 * <b>integerResult</b>: emits Integer<br>
 * <b>longResult</b>: emits Long<br>
 * <br>
 * @displayName Sigma
 * @category Math
 * @tags aggregate, numeric, collection
 * @param <T>
 * @since 0.3.3
 */
@OperatorAnnotation(partitionable = false)
public class Sigma<T extends Number> extends AbstractAggregateCalc<T>
{
  @Override
  public long aggregateLongs(Collection<T> collection)
  {
    long l = 0;

    for (Number n : collection) {
      l += n.longValue();
    }

    return l;
  }

  @Override
  public double aggregateDoubles(Collection<T> collection)
  {
    double d = 0;

    for (Number n : collection) {
      d += n.doubleValue();
    }

    return d;
  }
}
