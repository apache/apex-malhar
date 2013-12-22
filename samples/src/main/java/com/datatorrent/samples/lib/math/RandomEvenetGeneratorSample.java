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
package com.datatorrent.samples.lib.math;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.*;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

/**
 * <b> Usage Operator : </b> com.datatorrent.lib.testbench.RandomEventGenerator <br>
 * This sample usage for predefined operator <b>RandomEventGenerator</b>. <br>
 * Random generator output is printed to output console(can be any downstream operator).
 *
 * @since 0.3.2
 */
public class RandomEvenetGeneratorSample implements StreamingApplication
{
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
		// Create application dag.
	    dag.setAttribute(DAG.APPLICATION_NAME, "RandomEventGeneratorApplication");
	    dag.setAttribute(DAG.DEBUG, true);

	    // Add random integer generator operator
	    RandomEventGenerator rand = dag.addOperator("rand", RandomEventGenerator.class);
	    rand.setMaxvalue(999999999);
	    rand.setTuplesBlast(10);
	    rand.setTuplesBlastIntervalMillis(1000);

	    // Connect to output console operator
	    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
	    dag.addStream("rand_console",rand.integer_data , console.input);

	    // done
	}


}
