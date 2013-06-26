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

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.script.PythonOperator;

/**
 * Unit test for PythonOperator.
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
