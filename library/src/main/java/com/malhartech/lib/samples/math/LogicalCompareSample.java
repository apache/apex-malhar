package com.malhartech.lib.samples.math;

import org.apache.hadoop.conf.Configuration;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.math.LogicalCompareToConstant;
import com.malhartech.lib.testbench.RandomEventGenerator;

/**
 * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> LogicalCompareToConstant <br>
 * <bClass : </b> com.malhartech.lib.math.LogicalCompareToConstant
 *
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class LogicalCompareSample implements ApplicationFactory
{
	@SuppressWarnings("unchecked")
	@Override
	public void getApplication(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAG.STRAM_APPNAME, "TestApp");
		dag.setAttribute(DAG.STRAM_DEBUG, true);

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
