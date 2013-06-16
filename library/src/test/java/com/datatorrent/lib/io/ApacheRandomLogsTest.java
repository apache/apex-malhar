/*
 *  Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.malhartech.lib.io;

import junit.framework.Assert;
import org.junit.Test;
import com.malhartech.engine.TestSink;
import com.malhartech.lib.io.ApacheGenRandomLogs;

/**
 * Unit test for emit tuples.
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class ApacheRandomLogsTest
{
	@Test
	public void test()
	{
		ApacheGenRandomLogs oper = new ApacheGenRandomLogs();
		TestSink sink = new TestSink();
		oper.outport.setSink(sink);
		oper.setup(null);
		
		Thread t = new EmitTuples(oper);
		t.start();
		try
		{
			Thread.sleep(1000);
		} catch (InterruptedException e)
		{
		}
		t.stop();
		Assert.assertTrue("Tuples emitted", sink.collectedTuples.size() > 0);
		System.out.println(sink.collectedTuples.size());
	}
	
	private class EmitTuples extends Thread {
		private ApacheGenRandomLogs oper;
		public EmitTuples(ApacheGenRandomLogs oper)
		{
			this.oper = oper;
		}
		@Override
		public void run()
		{
			oper.emitTuples();
		}
	}
}
