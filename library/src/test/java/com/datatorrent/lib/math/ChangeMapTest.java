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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.ChangeMap}.
 * <p>
 *
 */
public class ChangeMapTest
{
	private static Logger log = LoggerFactory.getLogger(ChangeMapTest.class);

	/**
	 * Test node logic emits correct results.
	 */
	@Test
	public void testNodeProcessing() throws Exception
	{
		testNodeProcessingSchema(new ChangeMap<String, Integer>());
		testNodeProcessingSchema(new ChangeMap<String, Double>());
		testNodeProcessingSchema(new ChangeMap<String, Float>());
		testNodeProcessingSchema(new ChangeMap<String, Short>());
		testNodeProcessingSchema(new ChangeMap<String, Long>());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
  public <V extends Number> void testNodeProcessingSchema(
			ChangeMap<String, V> oper)
	{
		CollectorTestSink changeSink = new CollectorTestSink();
		CollectorTestSink percentSink = new CollectorTestSink();

		oper.change.setSink(changeSink);
		oper.percent.setSink(percentSink);

		oper.beginWindow(0);
		HashMap<String, V> input = new HashMap<String, V>();
		input.put("a", oper.getValue(2));
		input.put("b", oper.getValue(10));
		input.put("c", oper.getValue(100));
		oper.base.process(input);

		input.clear();
		input.put("a", oper.getValue(3));
		input.put("b", oper.getValue(2));
		input.put("c", oper.getValue(4));
		oper.data.process(input);

		input.clear();
		input.put("a", oper.getValue(4));
		input.put("b", oper.getValue(19));
		input.put("c", oper.getValue(150));
		oper.data.process(input);

		oper.endWindow();

		// One for each key
		Assert.assertEquals("number emitted tuples", 6,
				changeSink.collectedTuples.size());
		Assert.assertEquals("number emitted tuples", 6,
				percentSink.collectedTuples.size());

		double aval = 0;
		double bval = 0;
		double cval = 0;
		log.debug("\nLogging tuples");
		for (Object o : changeSink.collectedTuples) {
			HashMap<String, Number> map = (HashMap<String, Number>) o;
			Assert.assertEquals("map size", 1, map.size());
			Number anum = map.get("a");
			Number bnum = map.get("b");
			Number cnum = map.get("c");
			if (anum != null) {
				aval += anum.doubleValue();
			}
			if (bnum != null) {
				bval += bnum.doubleValue();
			}
			if (cnum != null) {
				cval += cnum.doubleValue();
			}
		}
		Assert.assertEquals("change in a", 3.0, aval,0);
		Assert.assertEquals("change in a", 1.0, bval,0);
		Assert.assertEquals("change in a", -46.0, cval,0);

		aval = 0.0;
		bval = 0.0;
		cval = 0.0;

		for (Object o : percentSink.collectedTuples) {
			HashMap<String, Number> map = (HashMap<String, Number>) o;
			Assert.assertEquals("map size", 1, map.size());
			Number anum = map.get("a");
			Number bnum = map.get("b");
			Number cnum = map.get("c");
			if (anum != null) {
				aval += anum.doubleValue();
			}
			if (bnum != null) {
				bval += bnum.doubleValue();
			}
			if (cnum != null) {
				cval += cnum.doubleValue();
			}
		}
		Assert.assertEquals("change in a", 150.0, aval,0);
		Assert.assertEquals("change in a", 10.0, bval,0);
		Assert.assertEquals("change in a", -46.0, cval,0);
	}
}
