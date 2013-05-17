package com.malhartech.demos.samples.algo;

import org.apache.hadoop.conf.Configuration;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.demos.samples.math.RandomKeyValMap;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.algo.BottomNMap;

/**
 * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> AverageMap <br>
 * <bClass : </b> com.malhartech.lib.math.AverageMap 
 * 
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class BottomNMapSample implements ApplicationFactory
{
	@SuppressWarnings("unchecked")
	@Override
	public DAG getApplication(Configuration conf) 
	{
		// Create application dag.
		DAG dag = new DAG(conf);
		dag.setAttribute(DAG.STRAM_APPNAME, "BottomNMapSample");
		//dag.setAttribute(DAG.STRAM_DEBUG, true);

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
		return dag;
	}
}
