package com.malhartech.lib.samplecode.algo;

import org.apache.hadoop.conf.Configuration;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.samplecode.math.RandomKeyValMap;
import com.malhartech.lib.algo.AllAfterMatchMap;

/**
 * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> AverageMap <br>
 * <bClass : </b> com.malhartech.lib.math.AverageMap
 *
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class AllAfterMatchMapSample implements ApplicationFactory
{
	@SuppressWarnings("unchecked")
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAG.STRAM_APPNAME, "AllAfterMatchMapSample");
		//dag.setAttribute(DAG.STRAM_DEBUG, true);

		// Add random integer generator operator
		RandomKeyValMap rand = dag.addOperator("rand", RandomKeyValMap.class);

		AllAfterMatchMap<String, Integer> allafter = dag.addOperator("average",
				AllAfterMatchMap.class);
		allafter.setValue(50);
		allafter.setTypeLTE();
		dag.addStream("stream1", rand.outport, allafter.data);
		dag.getMeta(allafter).getAttributes()
				.attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(20);

		// Connect to output console operator
		ConsoleOutputOperator console = dag.addOperator("console",
				new ConsoleOutputOperator());
		dag.addStream("consolestream", allafter.allafter, console.input);

		// done
	}
}
