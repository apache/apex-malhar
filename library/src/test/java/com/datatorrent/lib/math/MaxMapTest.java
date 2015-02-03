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

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.MaxMap}.
 *
 */
public class MaxMapTest
{
	/**
	 * Test functional logic.
	 */
	@Test
	public void testNodeProcessing()
	{
		testSchemaNodeProcessing(new MaxMap<String, Integer>(), "integer");
		testSchemaNodeProcessing(new MaxMap<String, Double>(), "double");
		testSchemaNodeProcessing(new MaxMap<String, Long>(), "long");
		testSchemaNodeProcessing(new MaxMap<String, Short>(), "short");
		testSchemaNodeProcessing(new MaxMap<String, Float>(), "float");
	}

	/**
	 * Test operator logic emits correct results for each schema.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testSchemaNodeProcessing(MaxMap oper, String type)
	{
		CountAndLastTupleTestSink maxSink = new CountAndLastTupleTestSink();
		oper.max.setSink(maxSink);

		oper.beginWindow(0);

		int numtuples = 10000;
		// For benchmark do -> numtuples = numtuples * 100;
		if (type.equals("integer")) {
			HashMap<String, Integer> tuple;
			for (int i = 0; i < numtuples; i++) {
				tuple = new HashMap<String, Integer>();
				tuple.put("a", new Integer(i));
				oper.data.process(tuple);
			}
		} else if (type.equals("double")) {
			HashMap<String, Double> tuple;
			for (int i = 0; i < numtuples; i++) {
				tuple = new HashMap<String, Double>();
				tuple.put("a", new Double(i));
				oper.data.process(tuple);
			}
		} else if (type.equals("long")) {
			HashMap<String, Long> tuple;
			for (int i = 0; i < numtuples; i++) {
				tuple = new HashMap<String, Long>();
				tuple.put("a", new Long(i));
				oper.data.process(tuple);
			}
		} else if (type.equals("short")) {
			HashMap<String, Short> tuple;
			int count = numtuples / 1000; // cannot cross 64K
			for (short j = 0; j < count; j++) {
				tuple = new HashMap<String, Short>();
				tuple.put("a", new Short(j));
				oper.data.process(tuple);

			}
		} else if (type.equals("float")) {
			HashMap<String, Float> tuple;
			for (int i = 0; i < numtuples; i++) {
				tuple = new HashMap<String, Float>();
				tuple.put("a", new Float(i));
				oper.data.process(tuple);
			}
		}
		oper.endWindow();

		Assert.assertEquals("number emitted tuples", 1, maxSink.count);
		Number val = ((HashMap<String, Number>) maxSink.tuple).get("a").intValue();
		if (type.equals("short")) {
			Assert.assertEquals("emitted max value was ", (new Double(
					numtuples / 1000 - 1)).intValue(), val);
		} else {
			Assert.assertEquals("emitted max value was ",
					(new Double(numtuples - 1)).intValue(), val);
		}
	}

	/**
	 * Tuple generator to test partitioning.
	 */
	public static class TestInputOperator extends BaseOperator implements
			InputOperator
	{
		public final transient DefaultOutputPort<HashMap<String, Integer>> output = new DefaultOutputPort<HashMap<String, Integer>>();
		public transient boolean first = true;

		@Override
		public void emitTuples()
		{
			if (first) {
				int i = 0;
				for (; i < 60; i++) {
					HashMap<String, Integer> tuple = new HashMap<String, Integer>();
					tuple.put("a", new Integer(i));
					tuple.put("b", new Integer(i));
					tuple.put("c", new Integer(i));
					output.emit(tuple);
				}
				for (; i < 80; i++) {
					HashMap<String, Integer> tuple = new HashMap<String, Integer>();
					tuple.put("a", new Integer(i));
					tuple.put("b", new Integer(i));
					output.emit(tuple);
				}
				for (; i < 100; i++) {
					HashMap<String, Integer> tuple = new HashMap<String, Integer>();
					tuple.put("a", new Integer(i));
					output.emit(tuple);
				}
				// a = 0..99, b = 0..79, c = 0..59
				first = false;
			}
		}
	}

	/**
	 * Tuple collector to test partitioning.
	 */
	public static class CollectorOperator extends BaseOperator
	{
		public static final ArrayList<HashMap<String, Integer>> buffer = new ArrayList<HashMap<String, Integer>>();
		public final transient DefaultInputPort<HashMap<String, Integer>> input = new DefaultInputPort<HashMap<String, Integer>>()
		{
			@Override
			public void process(HashMap<String, Integer> tuple)
			{
				buffer.add(tuple);
			}
		};
	}
}
