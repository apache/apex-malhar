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

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

/**
 * 
 * Functional tests for {@link com.datatorrent.lib.math.MinMap}
 * <p>
 * 
 */
public class MinMapTest
{
	/**
	 * Test functional logic
	 */
	@Test
	public void testNodeProcessing() throws InterruptedException
	{
		testSchemaNodeProcessing(new MinMap<String, Integer>(), "integer"); // 8million/s
		testSchemaNodeProcessing(new MinMap<String, Double>(), "double"); // 8
																																			// million/s
		testSchemaNodeProcessing(new MinMap<String, Long>(), "long"); // 8 million/s
		testSchemaNodeProcessing(new MinMap<String, Short>(), "short"); // 8
																																		// million/s
		testSchemaNodeProcessing(new MinMap<String, Float>(), "float"); // 8
																																		// million/s
	}

	/**
	 * Test oper logic emits correct results for each schema
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testSchemaNodeProcessing(MinMap oper, String type)
			throws InterruptedException
	{
		CountAndLastTupleTestSink minSink = new CountAndLastTupleTestSink();
		oper.min.setSink(minSink);

		oper.beginWindow(0);
		int numtuples = 100;
		// For benchmark do -> numtuples = numtuples * 100;
		if (type.equals("integer")) {
			HashMap<String, Integer> tuple = new HashMap<String, Integer>();
			for (int i = 0; i < numtuples; i++) {
				tuple.put("a", new Integer(i));
				oper.data.process(tuple);
			}
		} else if (type.equals("double")) {
			HashMap<String, Double> tuple = new HashMap<String, Double>();
			for (int i = 0; i < numtuples; i++) {
				tuple.put("a", new Double(i));
				oper.data.process(tuple);
			}
		} else if (type.equals("long")) {
			HashMap<String, Long> tuple = new HashMap<String, Long>();
			for (int i = 0; i < numtuples; i++) {
				tuple.put("a", new Long(i));
				oper.data.process(tuple);
			}
		} else if (type.equals("short")) {
			HashMap<String, Short> tuple = new HashMap<String, Short>();
			for (short i = 0; i < numtuples; i++) {
				tuple.put("a", new Short(i));
				oper.data.process(tuple);
			}
		} else if (type.equals("float")) {
			HashMap<String, Float> tuple = new HashMap<String, Float>();
			for (int i = 0; i < numtuples; i++) {
				tuple.put("a", new Float(i));
				oper.data.process(tuple);
			}
		}
		oper.endWindow();

		Assert.assertEquals("number emitted tuples", 1, minSink.count);
		HashMap<String, Number> shash = (HashMap<String, Number>) minSink.tuple;
		Number val = shash.get("a").intValue();
		Assert.assertEquals("number emitted tuples", 1, shash.size());
		Assert.assertEquals("emitted min value was ", 0, val);
	}
}
