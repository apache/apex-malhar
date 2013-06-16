package com.datatorrent.lib.samplecode.math;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.math.CountKeyVal;
import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;

/**
 * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> CountKeyVal <br>
 * <bClass : </b> com.datatorrent.lib.math.CountKeyVal
 *
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class CountKeyValSample implements ApplicationFactory
{
	@SuppressWarnings("unchecked")
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAG.APPLICATION_NAME, "TestApp");
		dag.setAttribute(DAG.DEBUG, true);

		// Add random integer generator operator
		CountKeyValues rand = dag.addOperator("rand", CountKeyValues.class);

		CountKeyVal<String, Integer> count = dag.addOperator("count",
				CountKeyVal.class);
		dag.addStream("stream1", rand.outport, count.data);
		dag.getMeta(count).getAttributes()
				.attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(50);

		// Connect to output console operator
		ConsoleOutputOperator console = dag.addOperator("console",
				new ConsoleOutputOperator());
		dag.addStream("consolestream", count.count, console.input);

		// done
	}
}
