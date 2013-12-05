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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * Calculate the running average of the input numbers and emit it at the end of
 * the window. This is an end of window operator<br>
 * <br>
 * <b>StateFull : Yes</b>, average is computed over application window. <br>
 * <b>Partitions : No</b>, will yield wrong results. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>input</b>: expects Number<br>
 * <b>longAverage</b>: emits Long<br>
 * <b>integerAverage</b>: emits Integer<br>
 * <b>doubleAverage</b>: emits Double<br>
 * <b>floatAverage</b>: emits Float<br>
 * <br>
 *
 * @since 0.3.3
 */
@OperatorAnnotation(partitionable = false)
public class RunningAverage extends BaseOperator
{
	/**
	 * Computed average.
	 */
	double average;

	/**
	 * Number of values on input port.
	 */
	long count;

	/**
	 * Input number port.
	 */
	public final transient DefaultInputPort<Number> input = new DefaultInputPort<Number>()
	{
		@Override
		public void process(Number tuple)
		{
			double sum = (count * average) + tuple.doubleValue();
			count++;
			average = sum / count;
		}
	};

	/**
	 * Double average output port.
	 */
	public final transient DefaultOutputPort<Double> doubleAverage = new DefaultOutputPort<Double>();

	/**
	 * Float average output port.
	 */
	public final transient DefaultOutputPort<Float> floatAverage = new DefaultOutputPort<Float>();

	/**
	 * Long average output port.
	 */
	public final transient DefaultOutputPort<Long> longAverage = new DefaultOutputPort<Long>();

	/**
	 * Integer average output port.
	 */
	public final transient DefaultOutputPort<Integer> integerAverage = new DefaultOutputPort<Integer>();

	/**
	 * End window operator override.
	 */
	@Override
	public void endWindow()
	{
		if (doubleAverage.isConnected()) {
			doubleAverage.emit(average);
		}

		if (floatAverage.isConnected()) {
			floatAverage.emit((float) average);
		}

		if (longAverage.isConnected()) {
			longAverage.emit((long) average);
		}

		if (integerAverage.isConnected()) {
			integerAverage.emit((int) average);
		}
	}
}
