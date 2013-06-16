/*
 *  Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.malhartech.lib.math;

import junit.framework.Assert;

import com.malhartech.engine.TestSink;
import com.malhartech.lib.testbench.CountOccurance;

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
