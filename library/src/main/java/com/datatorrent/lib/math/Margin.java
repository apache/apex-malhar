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
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberValueOperator;

/**
 * <p>
 * Operator sums numerator and denominator value arriving at input ports. <br>
 * Margin Formula : (1 - numerator/denominator). <br>
 * If percent flag is set than margin is emitted as percentage. <br>
 * <br>
 * StateFull : Yes, numerator and denominator are summed for application
 * windows. <br>
 * Partitions : No, will yield worng margin result, no unifier on output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>numerator</b>: expects V extends Number<br>
 * <b>denominator</b>: expects V extends Number<br>
 * <b>margin</b>: emits Double<br>
 * <br>
 * <b>Properties:<b>
 * <br>
 * <b>percent: </b>  output margin as percentage value.
 *
 * @since 0.3.3
 */
@OperatorAnnotation(partitionable = false)
public class Margin<V extends Number> extends BaseNumberValueOperator<V>
{
	/**
	 * Sum of numerator values.
	 */
	protected double nval = 0.0;

	/**
	 * sum of denominator values.
	 */
	protected double dval = 0.0;

	/**
	 * Flag to output margin as percentage.
	 */
	protected boolean percent = false;

	/**
	 * Numerator input port.
	 */
	@InputPortFieldAnnotation(name = "numerator")
	public final transient DefaultInputPort<V> numerator = new DefaultInputPort<V>()
	{
		/**
		 * Adds to the numerator value
		 */
		@Override
		public void process(V tuple)
		{
			nval += tuple.doubleValue();
		}
	};

	/**
	 * Denominator input port.
	 */
	@InputPortFieldAnnotation(name = "denominator")
	public final transient DefaultInputPort<V> denominator = new DefaultInputPort<V>()
	{
		/**
		 * Adds to the denominator value
		 */
		@Override
		public void process(V tuple)
		{
			dval += tuple.doubleValue();
		}
	};

	/**
	 * Output margin port.
	 */
	@OutputPortFieldAnnotation(name = "margin")
	public final transient DefaultOutputPort<V> margin = new DefaultOutputPort<V>();

	/**
	 * getter function for percent
	 * 
	 * @return percent
	 */
	public boolean getPercent()
	{
		return percent;
	}

	/**
	 * setter function for percent
	 * 
	 * @param val
	 *          sets percent
	 */
	public void setPercent(boolean val)
	{
		percent = val;
	}

	/**
	 * Generates tuple emits it as long as denomitor is not 0 Clears internal data
	 */
	@Override
	public void endWindow()
	{
		if (dval == 0) {
			return;
		}
		double val = 1 - (nval / dval);
		if (percent) {
			val = val * 100;
		}
		margin.emit(getValue(val));
		nval = 0.0;
		dval = 0.0;
	}
}
