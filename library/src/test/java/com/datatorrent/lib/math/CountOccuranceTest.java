/*
 *  Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.datatorrent.lib.math;

import junit.framework.Assert;

import com.datatorrent.lib.testbench.CountOccurance;
import com.malhartech.engine.TestSink;

import org.junit.Test;

/**
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class CountOccuranceTest
{
	@Test
	public void testProcess()
	{
		CountOccurance oper = new CountOccurance();
		oper.setup(null);
		TestSink sink = new TestSink();
    oper.outport.setSink(sink);
    
    oper.beginWindow(1);
    oper.inport.process("a");
    oper.inport.process("b");
    oper.endWindow();
    
    Assert.assertEquals("number emitted tuples", 1, sink.collectedTuples.size());
	}
}
