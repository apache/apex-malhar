package com.datatorrent.lib.logs;

import java.util.HashMap;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.algo.TopNUniqueTest;
import com.datatorrent.lib.logs.MultiWindowDimensionAggregation;
import com.datatorrent.lib.testbench.CollectorTestSink;

public class MultiWindowDimensionAggregationTest {

	private static Logger log = LoggerFactory.getLogger(TopNUniqueTest.class);

	/**
	 * Test node logic emits correct results
	 */
	@Test
	public void testNodeProcessing() throws Exception {
		testNodeProcessingSchema(new MultiWindowDimensionAggregation());

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void testNodeProcessingSchema(MultiWindowDimensionAggregation oper) {
		oper.setup(null);
		oper.setWindowSize(3);
		oper.setDimension("url");
		CollectorTestSink sortSink = new CollectorTestSink();
		oper.output.setSink(sortSink);
		
		oper.beginWindow(0);
		HashMap<String, String> input = new HashMap<String, String>();

		input.put("url","abc=10:def=20:ghi=10");
		oper.data.process(input);
		oper.endWindow();

		input.clear();
		oper.beginWindow(1);
		input.put("url", "abc=34:pqr=30");
		oper.data.process(input);
		oper.endWindow();
		
		input.clear();
		oper.beginWindow(2);
		input.put("url", "pqr=30");
		oper.data.process(input);
		oper.endWindow();
		
		Assert.assertEquals("number emitted tuples", 11,	sortSink.collectedTuples.size());
		for (Object o : sortSink.collectedTuples) {
			log.debug(o.toString());
		}
		log.debug("Done testing round\n");
	}
}
