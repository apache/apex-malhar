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

import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAGContext;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.HttpJsonChunksInputOperator;

/**
 * * This sample application code for showing sample usage of malhar
 * operator(s). <br>
 * <b>Operator : </b> HttpInputOperator <br>
 * <bClass : </b> com.datatorrent.lib.io.HttpJsonChunksInputOperator
 *
 * this application connects to yahoo news and relays raw content to output console.
 *
 * @since 0.3.2
 */
public class HttpInputOperatorSample implements StreamingApplication
{
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAGContext.APPLICATION_NAME, "HttpInputOperatorApplication");
		dag.setAttribute(DAGContext.DEBUG, true);

		// Add random integer generator operator
		HttpJsonChunksInputOperator reader = dag.addOperator("reader",
				HttpJsonChunksInputOperator.class);
		reader.setUrl(URI.create("http://news.yahoo.com"));
		reader.readTimeoutMillis = 10000;

		// Connect to output console operator
		ConsoleOutputOperator console = dag.addOperator("console",
				new ConsoleOutputOperator());
		dag.addStream("consoleout", reader.rawOutput, console.input);

		// done
	}

}
