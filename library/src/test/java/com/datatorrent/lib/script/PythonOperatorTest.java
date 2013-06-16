/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.script;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.datatorrent.lib.script.PythonOperator;
import com.malhartech.engine.TestSink;

/**
 * Unit test for PythonOperator.
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 *
 */
public class PythonOperatorTest
{
	@Test
	public void testJavaOperator()
	{
		PythonOperator oper= new PythonOperator();
		StringBuilder builder = new StringBuilder();
		builder.append("import operator\n").append("val = operator.mul(val, val)");
		oper.setScript(builder.toString());
		oper.setPassThru(true);
		
		TestSink sink = new TestSink();
		oper.result.setSink(sink);
    HashMap<String, Object> tuple = new HashMap<String, Object>();
    tuple.put("val", new Integer(2));
    
		oper.beginWindow(0); 
		oper.inBindings.process(tuple);
		oper.endWindow();
             
		Assert.assertEquals("number emitted tuples", 1, sink.collectedTuples.size());
	  for (Object o: sink.collectedTuples) { // count is 12
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
