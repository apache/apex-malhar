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

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberValueOperator;
import com.datatorrent.lib.util.HighLow;
import com.datatorrent.lib.util.UnifierRange;

/**
 * <p>
 * Emits the range of values at the end of window<br>
 * <br>
 * <b>StateFull : Yes</b>, values are computed over application time window. <br>
 * <b>Partitions : Yes </b>, High/Low values are unified on output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>range</b>: emits HighLow&lt;V&gt;<br>
 * <br>
 * <br>
 *
 * @since 0.3.3
 */
public class Range<V extends Number> extends BaseNumberValueOperator<V>
{
	/**
	 * Highest value on input port.
	 */
	protected V high = null;

	/**
	 * Lowest value on input port.
	 */
	protected V low = null;

	/**
	 * Input data port.
	 */
	@InputPortFieldAnnotation(name = "data")
	public final transient DefaultInputPort<V> data = new DefaultInputPort<V>()
	{
		/**
		 * Process each tuple to compute new high and low
		 */
		@Override
		public void process(V tuple)
		{
			if ((low == null) || (low.doubleValue() > tuple.doubleValue())) {
				low = tuple;
			}

			if ((high == null) || (high.doubleValue() < tuple.doubleValue())) {
				high = tuple;
			}
		}
	};

	/**
	 * Output range port, with high low unifier operator.
	 */
	@OutputPortFieldAnnotation(name = "range")
	public final transient DefaultOutputPort<HighLow<V>> range = new DefaultOutputPort<HighLow<V>>()
	{
		@Override
		public Unifier<HighLow<V>> getUnifier()
		{
			return new UnifierRange<V>();
		}
	};

	/**
	 * Emits the range. If no tuple was received in the window, no emit is done
	 * Clears the internal data before return
	 */
	@Override
	public void endWindow()
	{
		if ((low != null) && (high != null)) {
			HighLow tuple = new HighLow(getValue(high.doubleValue()),
					getValue(low.doubleValue()));
			range.emit(tuple);
		}
		high = null;
		low = null;
	}
}
