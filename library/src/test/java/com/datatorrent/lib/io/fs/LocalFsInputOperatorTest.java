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
package com.datatorrent.lib.io.fs;

import com.datatorrent.lib.testbench.CollectorTestSink;
import org.junit.Assert;
import org.junit.Test;

/**
 * Functional tests for {@link com.datatorrent.lib.io.fs.LocalFsInputOperator} <p>
 */
public class LocalFsInputOperatorTest
{
	// Sample text file path.
	protected String fileName = "../demos/wordcount/src/main/resources/samplefile.txt";

	@SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
	public void testFileRead()
	{
		LocalFsInputOperator oper = new LocalFsInputOperator();
		oper.setFilePath(fileName);
		oper.setup(null);
		oper.activate(null);
		CollectorTestSink sink = new CollectorTestSink();
		oper.outport.setSink(sink);
		for(int i=0; i < 1000; i++) oper.emitTuples();
		Assert.assertTrue("tuple emmitted", sink.collectedTuples.size() > 0);
		Assert.assertEquals(sink.collectedTuples.size(), 92);
		oper.deactivate();
		oper.teardown();
	}
}
