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
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.datatorrent.api.Sink;
import com.datatorrent.lib.util.HighLow;

/**
 * Functional tests for {@link com.datatorrent.lib.math.Range}
 */
public class RangeTest<V extends Number>
{
	@SuppressWarnings("rawtypes")
	class TestSink implements Sink
	{
		List<Object> collectedTuples = new ArrayList<Object>();

		@Override
		public void put(Object payload)
		{
          collectedTuples.add(payload);
		}

		@Override
		public int getCount(boolean reset)
		{
			throw new UnsupportedOperationException("Not supported yet.");
		}
	}

	/**
	 * Test oper logic emits correct results
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testNodeSchemaProcessing()
	{
		Range<Double> oper = new Range<Double>();
		TestSink rangeSink = new TestSink();
		oper.range.setSink(rangeSink);

		oper.beginWindow(0); //

		int numTuples = 1000;
		for (int i = 0; i < numTuples; i++) {
			Double a = new Double(20.0);
			Double b = new Double(2.0);
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
		}

		oper.endWindow(); //

		// payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
		Assert.assertEquals("number emitted tuples", 1,
				rangeSink.collectedTuples.size());
		for (Object o : rangeSink.collectedTuples) {
			HighLow<V> hl = (HighLow<V>) o;
			Assert.assertEquals("emitted high value was ", new Double(1000.0),
					hl.getHigh());
			Assert.assertEquals("emitted low value was ", new Double(1.0),
					hl.getLow());
		}
	}
}
