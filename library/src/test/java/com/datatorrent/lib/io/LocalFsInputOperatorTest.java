/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.engine.TestSink;
import junit.framework.Assert;
import org.junit.Test;

/**
 * Functional tests for {@link com.malhartech.lib.io.LocalFsInputOperator} <p>
 * @author Dinesh Prasad (dinesh@malhar-inc.com).
 */
public class LocalFsInputOperatorTest
{
	// Sample text file path.
	protected String fileName = "../demos/src/main/resources/com/malhartech/demos/wordcount/samplefile.txt";
	
	@Test
	public void testFileRead()
	{
		LocalFsInputOperator oper = new LocalFsInputOperator();
		oper.setFilePath(fileName);
		oper.setup(null);
		TestSink sink = new TestSink();
		oper.outport.setSink(sink);
		for(int i=0; i < 1000; i++) oper.emitTuples();
		Assert.assertTrue("tuple emmitted", sink.collectedTuples.size() > 0);
		Assert.assertEquals(sink.collectedTuples.size(), 92);
	}
}
