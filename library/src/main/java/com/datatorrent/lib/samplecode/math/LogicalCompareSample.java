package com.datatorrent.lib.samplecode.math;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.math.LogicalCompareToConstant;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;

/**
 * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> LogicalCompareToConstant <br>
 * <bClass : </b> com.datatorrent.lib.math.LogicalCompareToConstant
 *
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class LogicalCompareSample implements ApplicationFactory
{
	@SuppressWarnings("unchecked")
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAG.APPLICATION_NAME, "TestApp");
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
