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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;

import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.math.Average;
import com.datatorrent.lib.testbench.RandomEventGenerator;

/**
 * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> Average <br>
 * <bClass : </b> com.datatorrent.lib.math.Average
 *
 * @since 0.3.2
 */
public class AverageSample implements StreamingApplication
{
	@SuppressWarnings("unchecked")
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAG.APPLICATION_NAME, "AverageSampleApplication");
		dag.setAttribute(DAG.DEBUG, true);

		// Add random integer generator operator
		RandomEventGenerator rand = dag.addOperator("rand",
				RandomEventGenerator.class);
		rand.setMaxvalue(1000);
		rand.setTuplesBlast(10);
		rand.setTuplesBlastIntervalMillis(1000);

		Average<Integer> average = dag.addOperator("average",
				Average.class);
		dag.addStream("stream1", rand.integer_data, average.data);
		dag.getMeta(average).getAttributes()
				.put(OperatorContext.APPLICATION_WINDOW_COUNT, 20);

		// Connect to output console operator
		ConsoleOutputOperator console = dag.addOperator("console",
				new ConsoleOutputOperator());
		dag.addStream("stream2", average.average, console.input);

		// done
	}

}
