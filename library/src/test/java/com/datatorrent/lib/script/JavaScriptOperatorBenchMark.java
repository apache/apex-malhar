/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Functional tests for {@link com.datatorrent.lib.script.BashOperator}.
 */
public class JavaScriptOperatorBenchMark
{
	private static Logger log = LoggerFactory.getLogger(JavaScriptOperatorBenchMark.class);

	@SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
	public void testJavaOperator()
	{
		// Create bash operator instance (calculate suqare).
		JavaScriptOperator oper = new JavaScriptOperator();
		StringBuilder builder = new StringBuilder();
		builder.append("val = val * val;");
		oper.addSetupScript("function square() { return val*val;}");
		oper.setup(null);
		oper.setInvoke("square");
		oper.setPassThru(true);
		CollectorTestSink sink = new CollectorTestSink();
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
	};
}
