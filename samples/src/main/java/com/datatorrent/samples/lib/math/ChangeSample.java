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

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.math.Change;
import com.datatorrent.lib.testbench.RandomEventGenerator;

/**
 *  This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> Change <br>
 * <bClass : </b> com.datatorrent.lib.math.Change
 *
 * @since 0.3.2
 */
public class ChangeSample implements StreamingApplication
{
	@SuppressWarnings("unchecked")
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAG.APPLICATION_NAME, "ChangeSampleApplication");
		dag.setAttribute(DAG.DEBUG, true);

		// Add random integer generator operator
		RandomEventGenerator rand = dag.addOperator("rand",
				RandomEventGenerator.class);
		rand.setMaxvalue(999999999);
		rand.setTuplesBlast(10);
		rand.setTuplesBlastIntervalMillis(1000);
		RandomEventGenerator rand1 = dag.addOperator("rand1",
				RandomEventGenerator.class);
		rand.setTuplesBlast(1);
		rand.setTuplesBlastIntervalMillis(5000);

		// append change operator
		Change<Integer> change = dag.addOperator("change", Change.class);
		dag.addStream("stream", rand1.integer_data, change.base);
		dag.addStream("stream1", rand.integer_data, change.data);

		// Connect to output console operator
		ConsoleOutputOperator console = dag.addOperator("console",
				new ConsoleOutputOperator());
		dag.addStream("consoleout", change.percent, console.input);

		// done
	}

}
