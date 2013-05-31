/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.script;

import com.malhartech.engine.TestSink;
import com.malhartech.lib.algo.AllAfterMatchMapBenchmark;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance tests for {@link com.malhartech.lib.script.BashOperator}. <p>
 * Testing with 1M tuples.
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 *
 */
public class PythonOperatorBanchmark
{
	private static Logger log = LoggerFactory.getLogger(AllAfterMatchMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
  	PythonOperator oper= new PythonOperator();
		StringBuilder builder = new StringBuilder();
		builder.append("import operator\n").append("val = operator.mul(val, val)");
		oper.setScript(builder.toString());
		oper.setPassThru(true);
		TestSink sink = new TestSink();
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
