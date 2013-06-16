package com.datatorrent.lib.samplecode.algo;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.algo.BottomNMap;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.samplecode.math.RandomKeyValMap;

/**
 * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> AverageMap <br>
 * <bClass : </b> com.datatorrent.lib.math.AverageMap
 *
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class BottomNMapSample implements StreamingApplication
{
	@SuppressWarnings("unchecked")
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAG.APPLICATION_NAME, "BottomNMapSample");
		//dag.setAttribute(DAG.DEBUG, true);

		// Add random integer generator operator
		RandomKeyValMap rand = dag.addOperator("rand", RandomKeyValMap.class);

		BottomNMap<String, Integer> bottomn = dag.addOperator("average",	BottomNMap.class);
		bottomn.setN(5);
		dag.addStream("stream1", rand.outport, bottomn.data);
		dag.getMeta(bottomn).getAttributes()
				.attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(100);

		// Connect to output console operator
		ConsoleOutputOperator console = dag.addOperator("console",
				new ConsoleOutputOperator());
		dag.addStream("consolestream", bottomn.bottom, console.input);

		// done
	}
}
