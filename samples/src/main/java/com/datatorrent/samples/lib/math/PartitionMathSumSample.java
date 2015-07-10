/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.samples.lib.math;


import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;

import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.math.Sum;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

/**
 *  * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> Sum <br>
 * <bClass : </b> com.datatorrent.lib.math.Sum
 * Sum operator is partitioned into 4 operator, partitioning is allowed on this operator. <br>
 *
 * @since 0.3.2
 */
public class PartitionMathSumSample implements StreamingApplication
{
	@SuppressWarnings("unchecked")
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAG.APPLICATION_NAME, "PartitionMathSumApplication");
		dag.setAttribute(DAG.DEBUG, true);

		// Add random integer generator operator
		RandomEventGenerator rand = dag.addOperator("rand",
				RandomEventGenerator.class);
		rand.setMaxvalue(1000);
		rand.setTuplesBlast(10);
		rand.setTuplesBlastIntervalMillis(500);

		Sum<Integer> sum = dag.addOperator("sum", Sum.class);
		dag.addStream("stream1", rand.integer_data, sum.data);
		dag.getMeta(sum).getAttributes().put(OperatorContext.PARTITIONER, new StatelessPartitioner<Sum<Integer>>(4));
		dag.getMeta(sum).getAttributes()
				.put(OperatorContext.APPLICATION_WINDOW_COUNT, 20);

		// Connect to output console operator
		ConsoleOutputOperator console = dag.addOperator("console",
				new ConsoleOutputOperator());
		dag.addStream("stream2", sum.sum, console.input);

		// done
	}

}
