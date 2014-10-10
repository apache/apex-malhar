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

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.Average}.
 * <p> Sample values are processed after operator begin window.
 * Result is collected on output sink and compared.
 */
public class AverageTest
{
	/**
	 * Test operator logic emits correct results.
	 */
	@Test
	public void testNodeProcessing()
	{
		Average<Double> doper = new Average<Double>();
		Average<Float> foper = new Average<Float>();
		Average<Integer> ioper = new Average<Integer>();
		Average<Long> loper = new Average<Long>();
		Average<Short> soper = new Average<Short>();
		doper.setType(Double.class);
		foper.setType(Float.class);
		ioper.setType(Integer.class);
		loper.setType(Long.class);
		soper.setType(Short.class);

		testNodeSchemaProcessing(doper);
		testNodeSchemaProcessing(foper);
		testNodeSchemaProcessing(ioper);
		testNodeSchemaProcessing(loper);
		testNodeSchemaProcessing(soper);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testNodeSchemaProcessing(Average oper)
	{
		CollectorTestSink averageSink = new CollectorTestSink();
		oper.average.setSink(averageSink);

		oper.beginWindow(0); //

		Double a = new Double(2.0);
		Double b = new Double(20.0);
		Double c = new Double(1000.0);

		oper.data.process(a);
		oper.data.process(b);
		oper.data.process(c);

		a = 1.0;
		oper.data.process(a);
		a = 10.0;
		oper.data.process(a);
		b = 5.0;
		oper.data.process(b);

		b = 12.0;
		oper.data.process(b);
		c = 22.0;
		oper.data.process(c);
		c = 14.0;
		oper.data.process(c);

		a = 46.0;
		oper.data.process(a);
		b = 2.0;
		oper.data.process(b);
		a = 23.0;
		oper.data.process(a);

		oper.endWindow(); //

		Assert.assertEquals("number emitted tuples", 1,
				averageSink.collectedTuples.size());
		for (Object o : averageSink.collectedTuples) { // count is 12
			Integer val = ((Number) o).intValue();
			Assert.assertEquals("emitted average value was was ", new Integer(
					1157 / 12), val);
		}
	}
}
