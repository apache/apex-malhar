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
package com.datatorrent.lib.math;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Functional tests for {@link com.datatorrent.lib.math.MinKeyVal}.
 */
public class MinKeyValTest
{
	/**
	 * Test functional logic
	 */
	@Test
	public void testNodeProcessing()
	{
		testSchemaNodeProcessing(new MinKeyVal<String, Integer>(), "integer");
		testSchemaNodeProcessing(new MinKeyVal<String, Double>(), "double");
		testSchemaNodeProcessing(new MinKeyVal<String, Long>(), "long");
		testSchemaNodeProcessing(new MinKeyVal<String, Short>(), "short");
		testSchemaNodeProcessing(new MinKeyVal<String, Float>(), "float");
	}

	/**
	 * Test operator logic emits correct results for each schema.
	 *
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testSchemaNodeProcessing(MinKeyVal oper, String type)
	{
		CountAndLastTupleTestSink minSink = new CountAndLastTupleTestSink();
		oper.min.setSink(minSink);

		oper.beginWindow(0);

		int numtuples = 10000;
		if (type.equals("integer")) {
			for (int i = numtuples; i > 0; i--) {
				oper.data.process(new KeyValPair("a", new Integer(i)));
			}
		} else if (type.equals("double")) {
			for (int i = numtuples; i > 0; i--) {
				oper.data.process(new KeyValPair("a", new Double(i)));
			}
		} else if (type.equals("long")) {
			for (int i = numtuples; i > 0; i--) {
				oper.data.process(new KeyValPair("a", new Long(i)));
			}
		} else if (type.equals("short")) {
			for (short j = 1000; j > 0; j--) { // cannot cross 64K
				oper.data.process(new KeyValPair("a", new Short(j)));
			}
		} else if (type.equals("float")) {
			for (int i = numtuples; i > 0; i--) {
				oper.data.process(new KeyValPair("a", new Float(i)));
			}
		}

		oper.endWindow();

		Assert.assertEquals("number emitted tuples", 1, minSink.count);
		Number val = ((KeyValPair<String, Number>) minSink.tuple).getValue()
				.intValue();
		if (type.equals("short")) {
			Assert.assertEquals("emitted min value was ", 1, val);
		} else {
			Assert.assertEquals("emitted min value was ", 1, val);
		}
	}

	/**
	 * Used to test partitioning.
	 */
	public static class TestInputOperator extends BaseOperator implements
			InputOperator
	{
		public final transient DefaultOutputPort<KeyValPair<String, Integer>> output = new DefaultOutputPort<KeyValPair<String, Integer>>();
		private transient boolean first = true;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public void emitTuples()
		{
			if (first) {
				for (int i = 40; i < 100; i++) {
					output.emit(new KeyValPair("a", new Integer(i)));
				}
				for (int i = 50; i < 100; i++) {
					output.emit(new KeyValPair("b", new Integer(i)));
				}
				for (int i = 60; i < 100; i++) {
					output.emit(new KeyValPair("c", new Integer(i)));
				}
				first = false;
			}
		}
	}

	public static class CollectorOperator extends BaseOperator
	{
		public static final ArrayList<KeyValPair<String, Integer>> buffer = new ArrayList<KeyValPair<String, Integer>>();
		public final transient DefaultInputPort<KeyValPair<String, Integer>> input = new DefaultInputPort<KeyValPair<String, Integer>>()
		{
			@SuppressWarnings({ "unchecked", "rawtypes" })
			@Override
			public void process(KeyValPair<String, Integer> tuple)
			{
				buffer.add(new KeyValPair(tuple.getKey(), tuple.getValue()));
			}
		};
	}
}
