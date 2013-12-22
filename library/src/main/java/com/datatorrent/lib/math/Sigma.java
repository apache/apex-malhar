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
package com.datatorrent.lib.math;

import com.datatorrent.api.annotation.OperatorAnnotation;

import java.util.Collection;

/**
 *
 * Adds incoming tuple to the state. This is a stateful operator that never
 * flushes its state; i.e. the addition would go on forever. The result of each
 * addition is emitted on the four ports, namely \"doubleResult\",
 * \"floatResult\", \"integerResult\", \"longResult\". Input tuple object has to
 * be an implementation of the interface Collection&lt;T&gt;. Tuples are emitted
 * on the output ports only if they are connected. This is done to avoid the
 * cost of calling the functions when some ports are not connected.
 * <p>
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
 *
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
