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
package com.datatorrent.demos.mroperator;

import junit.framework.Assert;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.demos.mroperator.ReduceOperator;
import com.datatorrent.demos.mroperator.WordCount;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.KeyHashValPair;

public class ReduceOperatorTest {

	 private static Logger logger = LoggerFactory.getLogger(ReduceOperatorTest.class);

	/**
	 * Test node logic emits correct results
	 */
	@Test
	public void testNodeProcessing() throws Exception {
		testNodeProcessingSchema(new ReduceOperator<Text, IntWritable,Text, IntWritable>());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void testNodeProcessingSchema(ReduceOperator<Text, IntWritable,Text, IntWritable> oper) {

		oper.setReduceClass(WordCount.Reduce.class);
		oper.setConfigFile(null);
		oper.setup(null);

		CollectorTestSink sortSink = new CollectorTestSink();
    oper.output.setSink(sortSink);

		oper.beginWindow(0);
		oper.inputCount.process(new KeyHashValPair<Integer, Integer>(1, 1));
		oper.input.process(new KeyHashValPair<Text, IntWritable>(new Text("one"), new IntWritable(1)));
		oper.input.process(new KeyHashValPair<Text, IntWritable>(new Text("one"), new IntWritable(1)));
		oper.input.process(new KeyHashValPair<Text, IntWritable>(new Text("two"), new IntWritable(1)));
		oper.endWindow();

		oper.beginWindow(1);
		oper.input.process(new KeyHashValPair<Text, IntWritable>(new Text("one"), new IntWritable(1)));
		oper.input.process(new KeyHashValPair<Text, IntWritable>(new Text("two"), new IntWritable(1)));
		oper.input.process(new KeyHashValPair<Text, IntWritable>(new Text("two"), new IntWritable(1)));
		oper.inputCount.process(new KeyHashValPair<Integer, Integer>(1, -1));
		oper.endWindow();
		Assert.assertEquals("number emitted tuples", 2,	sortSink.collectedTuples.size());
		for (Object o : sortSink.collectedTuples) {
      logger.debug(o.toString());
    }
    logger.debug("Done testing round\n");
	}
}
