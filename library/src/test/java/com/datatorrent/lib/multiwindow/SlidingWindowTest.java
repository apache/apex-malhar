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
package com.datatorrent.lib.multiwindow;

import static junit.framework.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Lists;

/**
 * Unit tests for
 * {@link com.datatorrent.lib.multiwindow.AbstractSlidingWindow}.
 */
public class SlidingWindowTest
{

	public class TestSlidingWindow extends AbstractSlidingWindow<String, List<String>>
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
			out.emit(tuples);
			tuples = new ArrayList<String>();
		}

    @Override
    public List<String> createWindowState()
    {
      return tuples;
    }

	};

	/**
	 * Test functional logic
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
	public void testNodeProcessing() throws InterruptedException
	{
	  TestSlidingWindow oper = new TestSlidingWindow();

		CollectorTestSink swinSink = new CollectorTestSink();
		oper.out.setSink(swinSink);
		oper.setWindowSize(3);
		oper.setup(null);

		oper.beginWindow(0);
		oper.data.process("a0");
		oper.data.process("b0");
		oper.endWindow();

		oper.beginWindow(1);
		oper.data.process("a1");
		oper.data.process("b1");
		oper.endWindow();

		oper.beginWindow(2);
		oper.data.process("a2");
		oper.data.process("b2");
		oper.endWindow();

		oper.beginWindow(3);
		oper.data.process("a3");
		oper.data.process("b3");
		oper.endWindow();

		assertEquals("number emitted tuples", 4,
				swinSink.collectedTuples.size());
		
		assertEquals("Invalid second stream window state.", oper.getStreamingWindowState(1), Lists.newArrayList("a2", "b2"));
		assertEquals("Invalid expired stream window state.", oper.lastExpiredWindowState, Lists.newArrayList("a0", "b0"));

	}
}
