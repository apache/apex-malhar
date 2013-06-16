package com.datatorrent.lib.samplecode.math;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.math.CompareMap;
import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;


/**
 * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> CompareMap <br>
 * <bClass : </b> com.datatorrent.lib.math.CompareMap
 *
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class CompreMapSample implements ApplicationFactory
{
	@SuppressWarnings("unchecked")
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAG.APPLICATION_NAME, "MobileDevApplication");
		dag.setAttribute(DAG.DEBUG, true);

		// Add random integer generator operator
		RandomKeyValues rand = dag.addOperator("rand", RandomKeyValues.class);

		CompareMap<String, Integer> compare = dag.addOperator("compare",
				CompareMap.class);
		compare.setTypeLTE();
		compare.setValue(50);

		// Connect to output console operator
		ConsoleOutputOperator console = dag.addOperator("console",
				new ConsoleOutputOperator());
		dag.addStream("consolestream", compare.compare, console.input);

		// done
	}
}
