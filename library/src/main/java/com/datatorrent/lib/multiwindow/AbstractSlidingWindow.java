/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.multiwindow;

import javax.validation.constraints.Min;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

/**
 *
 * A sliding window class that lets users access past N-1 window states
 * <p>
 * N is a property. The default behavior is just a passthrough, i.e. the
 * operator does not do any processing on its own. Users are expected to extend
 * this class and add their specific processing. Users have to define their own
 * output port(s)<br>
 * This module is a pass through or end of window as per users choice<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects T (any POJO)<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>N</b>: Number of windows to keep state on<br>
 * <br>
 */
public abstract class AbstractSlidingWindow<T> extends BaseOperator
{
	@InputPortFieldAnnotation(name = "data")
	public final transient DefaultInputPort<T> data = new DefaultInputPort<T>()
	{
		@Override
		public void process(T tuple)
		{
			processDataTuple(tuple);
		}
	};

	protected Object[] states = null;
	protected int currentstate = -1;

	@Min(2)
	int n = 2;

	/**
	 * getter function for n (number of previous window states
	 *
	 * @return n
	 */
	@Min(2)
	public int getN()
	{
		return n;
	}

	/**
	 * setter function for n
	 *
	 * @param i
	 */
	void setN(int i)
	{
		n = i;
	}

	abstract void processDataTuple(T tuple);

	/**
	 * Saves Object o as current window state. Calling this api twice in same
	 * window would simply overwrite the previous call. This can be called during
	 * processDataTuple, or endWindow. Usually it is better to call it in
	 * endWindow.
	 *
	 * @param o
	 */
	public void saveWindowState(Object o)
	{
		states[currentstate] = o;
	}

	/**
	 * Can access any previous window state upto n-1 (0 is current, n-1 is the
	 * N-1th previous)
	 *
	 * @param i
	 *          the previous window whose state is accessed
	 * @return Object
	 */
	public Object getWindowState(int i)
	{
		if (i >= getN()) {
			return null;
		}
		int j = currentstate;
		while (i != (getN() - 1)) {
			j--;
			if (j < 0) {
				j = getN() - 1;
			}
			i++;
		}
		return states[j];
	}

	/**
	 * Moves states by 1 and sets current state to null. If you override
	 * beginWindow, you must call super.beginWindow(windowId) to ensure proper
	 * operator behavior.
	 *
	 * @param windowId
	 */
	@Override
	public void beginWindow(long windowId)
	{
		currentstate++;
		if (currentstate >= getN()) {
			for (int i=1; i < getN(); i++) {
				states[i-1] = states[i];
			}
			currentstate = getN()-1;
		}
		states[currentstate] = null;
	}

	/**
	 * Sets up internal state structure
	 *
	 * @param context
	 */
	@Override
	public void setup(OperatorContext context)
	{
		states = new Object[getN()];
		for (int i = 0; i < getN(); i++) {
			states[i] = null;
		}
		currentstate = -1;
	}
}
