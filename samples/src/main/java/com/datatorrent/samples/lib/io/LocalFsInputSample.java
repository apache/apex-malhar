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
package com.datatorrent.samples.lib.io;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.fs.LocalFsInputOperator;

/**
 * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> AverageKeyVal <br>
 * <bClass : </b> com.datatorrent.lib.math.AverageKeyVal
 * This application reads local demo text file and relays text content to output console.
 *
 * @since 0.3.2
 */
public class LocalFsInputSample implements StreamingApplication
{
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAG.APPLICATION_NAME, "LocalFsInputApplication");
		dag.setAttribute(DAG.DEBUG, true);

		// Add random integer generator operator
		LocalFsInputOperator reader = dag.addOperator("reader",
				LocalFsInputOperator.class);
		reader
				.setFilePath("src/main/resources/com/datatorrent/demos/wordcount/samplefile.txt");
		reader.setSleepInterval(1000);

		// Connect to output console operator
		ConsoleOutputOperator console = dag.addOperator("console",
				new ConsoleOutputOperator());
		dag.addStream("outstream", reader.outport, console.input);

		// done
	}
}
