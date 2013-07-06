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

import java.util.ArrayList;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Functional tests for {@link com.datatorrent.lib.multiwindow.AbstractSlidingWindow}.
 */
public class MultiWindowRangeKeyValTest
{
	private static Logger log = LoggerFactory.getLogger(MultiWindowRangeKeyValTest.class);

	public class mySlidingWindow extends AbstractSlidingWindow<String>
	{
		@OutputPortFieldAnnotation(name = "out")
		public final transient DefaultOutputPort<ArrayList<String>> out = new DefaultOutputPort<ArrayList<String>>();

		ArrayList<String> tuples = new ArrayList<String>();

		@Override
		void processDataTuple(String tuple)
		{
			tuples.add(tuple);
		}

		@Override
		public void endWindow()
		{
			saveWindowState(tuples);
			out.emit(tuples);
			tuples = new ArrayList<String>();
		}

		public void dumpStates()
		{
			log.debug("\nDumping states");
			int i = getN() - 1;
			while (i >= 0) {
				Object o = getWindowState(i);
				log.debug(String.format("State %d: %s", i, (o != null) ? o.toString()
						: "null"));
				i--;
			}
		}

	};

	/**
	 * Test functional logic
	 */
	@Test
	public void testNodeProcessing() throws InterruptedException
	{
		MultiWindowRangeKeyVal<String, Integer> oper = new MultiWindowRangeKeyVal<String, Integer>();

		TestSink swinSink = new TestSink();
		oper.range.setSink(swinSink);

		oper.beginWindow(0);
		KeyValPair<String, Integer> low = new KeyValPair<String, Integer>("a", 3);
		oper.data.process(low);
		KeyValPair<String, Integer> high = new KeyValPair<String, Integer>("a", 11);
		oper.data.process(high);
		oper.endWindow();

		oper.beginWindow(1);
		low = new KeyValPair<String, Integer>("a", 1);
		oper.data.process(low);
		high = new KeyValPair<String, Integer>("a", 9);
		oper.data.process(high);
		oper.endWindow();

		Assert.assertEquals("number emitted tuples", 1, swinSink.collectedTuples.size());
		for (Object o : swinSink.collectedTuples) {
			log.debug(o.toString());
		}
	}
}
