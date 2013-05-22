package com.malhartech.lib.samples.math;

import org.apache.hadoop.conf.Configuration;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.math.AverageMap;

/**
 * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> AverageMap <br>
 * <bClass : </b> com.malhartech.lib.math.AverageMap
 *
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class AverageKeyValMapSample  implements ApplicationFactory
{
	@SuppressWarnings("unchecked")
	@Override
	public void getApplication(DAG dag, Configuration conf)
	{
		// Create application dag.
	    dag.setAttribute(DAG.STRAM_APPNAME, "MobileDevApplication");
	    dag.setAttribute(DAG.STRAM_DEBUG, true);

	    // Add random integer generator operator
	    RandomKeyValMap rand = dag.addOperator("rand", RandomKeyValMap.class);

	    AverageMap<String, Integer> average = dag.addOperator("average", AverageMap.class);
	    dag.addStream("stream1",rand.outport , average.data);
	    dag.getMeta(average).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(20);

	    // Connect to output console operator
	    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
	    dag.addStream("consolestream", average.average, console.input);

	    // done
	}
}
