/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.script;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.malhartech.engine.TestSink;

/**
 *
 * Functional tests for {@link com.malhartech.lib.script.BashOperator}. <p>
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 *
 */
public class BashOperatorTest
{
	@Test
	public void testJavaOperator()
	{
		// Create bash operator instance (calculate suqare).
		BashOperator oper = new BashOperator();
		StringBuilder builder = new StringBuilder();
		builder.append("val = val * val;");
		oper.setScript(builder.toString());
		oper.setPassThru(true);
		TestSink sink = new TestSink();
		oper.result.setSink(sink);
		
		// Add input sample data.
		HashMap<String, Object> tuple = new HashMap<String, Object>();
		tuple.put("val", new Integer(2));

		// Process operator.
		oper.beginWindow(0);
		oper.inBindings.process(tuple);
		oper.endWindow();

		// Validate value.   
		Assert.assertEquals("number emitted tuples", 1, sink.collectedTuples.size());
		for (Object o : sink.collectedTuples)
		{ // count is 12
			@SuppressWarnings("unchecked")
			Map<String, Object> val = (Map<String, Object>) o;
			for (Map.Entry<String, Object> entry : val.entrySet())
			{
				Assert.assertEquals("emitted average value was was ", new Integer(4),
						(Integer) entry.getValue());
			}
		}
	};
}
