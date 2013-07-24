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

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Functional tests for {@link com.datatorrent.lib.script.BashOperator}.
 */
public class BashOperatorTest
{
	@SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
	public void testJavaOperator()
	{
		// Create bash operator instance (calculate suqare).
		BashOperator oper = new BashOperator();
		StringBuilder builder = new StringBuilder();
		builder.append("val = val * val;");
		oper.setScript(builder.toString());
		oper.setPassThru(true);
		CollectorTestSink sink = new CollectorTestSink();
		oper.result.setSink(sink);

		// Add input sample data.
		HashMap<String, Object> tuple = new HashMap<String, Object>();
		tuple.put("val", new Integer(2));

		// Process operator.
		oper.beginWindow(0);
		oper.inBindings.process(tuple);
		oper.endWindow();

		// Validate value.
		Assert
				.assertEquals("number emitted tuples", 1, sink.collectedTuples.size());
		for (Object o : sink.collectedTuples) { // count is 12
			Map<String, Object> val = (Map<String, Object>) o;
			for (Map.Entry<String, Object> entry : val.entrySet()) {
				Assert.assertEquals("emitted average value was was ", new Integer(4),
						(Integer) entry.getValue());
			}
		}
	};
}
