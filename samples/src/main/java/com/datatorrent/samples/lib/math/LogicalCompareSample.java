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
import com.datatorrent.lib.math.LogicalCompareToConstant;
import com.datatorrent.lib.testbench.RandomEventGenerator;

/**
 * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> LogicalCompareToConstant <br>
 * <bClass : </b> com.datatorrent.lib.math.LogicalCompareToConstant
 *
 * @since 0.3.2
 */
public class LogicalCompareSample implements StreamingApplication
{
	@SuppressWarnings("unchecked")
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAG.APPLICATION_NAME, "LogicalCompareApplication");
		dag.setAttribute(DAG.DEBUG, true);

		// Add random integer generator operator
		RandomEventGenerator rand = dag.addOperator("rand",
				RandomEventGenerator.class);

		LogicalCompareToConstant<Integer> compare = dag.addOperator("compare",
				LogicalCompareToConstant.class);
		compare.setConstant(50);
		dag.addStream("stream1", rand.integer_data, compare.input);

		// Connect to output console operator
		ConsoleOutputOperator console = dag.addOperator("console",
				new ConsoleOutputOperator());
		dag.addStream("consolestream", compare.lessThan, console.input);

		// done
	}
}
