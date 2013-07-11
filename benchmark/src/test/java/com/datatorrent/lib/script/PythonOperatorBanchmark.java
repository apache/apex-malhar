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
package com.datatorrent.lib.script;

import com.datatorrent.lib.algo.AllAfterMatchMapBenchmark;
import com.datatorrent.lib.script.PythonOperator;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance tests for {@link com.datatorrent.lib.script.BashOperator}. <p>
 * Testing with 1M tuples.
 *
 */
public class PythonOperatorBanchmark
{
	private static Logger log = LoggerFactory.getLogger(AllAfterMatchMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
  	PythonOperator oper= new PythonOperator();
		StringBuilder builder = new StringBuilder();
		builder.append("import operator\n").append("val = operator.mul(val, val)");
		oper.setScript(builder.toString());
		oper.setPassThru(true);
		CollectorTestSink sink = new CollectorTestSink();
		oper.result.setSink(sink);

	  // generate process tuples
		long startTime = System.nanoTime();
		oper.beginWindow(0);
		int numTuples = 10000000;
		for (int i = 0; i < numTuples; i++)
		{
			HashMap<String, Object> tuple = new HashMap<String, Object>();
			tuple.put("val", new Integer(i));
		}
		oper.endWindow();
		long endTime = System.nanoTime();
		long total = (startTime - endTime)/1000;
		log.debug(String.format("\nBenchmarked %d tuples in %d ms", numTuples, total));
  }
}
