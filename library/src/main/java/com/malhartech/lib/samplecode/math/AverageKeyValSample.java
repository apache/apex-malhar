package com.malhartech.lib.samplecode.math;

import org.apache.hadoop.conf.Configuration;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.math.AverageKeyVal;

/**
 * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> AverageKeyVal <br>
 * <bClass : </b> com.malhartech.lib.math.AverageKeyVal
 *
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class AverageKeyValSample implements ApplicationFactory
{
	@SuppressWarnings("unchecked")
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAG.STRAM_APPNAME, "AverageKeyValSample");
		dag.setAttribute(DAG.STRAM_DEBUG, true);

		// Add random integer generator operator
		RandomKeyValues rand = dag.addOperator("rand", RandomKeyValues.class);

		AverageKeyVal<String> average = dag.addOperator("average",
				AverageKeyVal.class);
		dag.addStream("stream1", rand.outport, average.data);
		dag.getMeta(average).getAttributes()
				.attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(20);

		// Connect to output console operator
		ConsoleOutputOperator console = dag.addOperator("console",
				new ConsoleOutputOperator());
		dag.addStream("consolestream", average.doubleAverage, console.input);

		// done
	}
}
