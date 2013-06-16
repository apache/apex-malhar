package com.datatorrent.lib.samplecode.math;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.*;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

/**
 * <b> Usage Operator : </b> com.datatorrent.lib.testbench.RandomEventGenerator <br>
 * This sample usage for predefined operator <b>RandomEventGenerator</b>. <br>
 * Random generator output is printed to output console(can be any downstream operator).
 *
 * @author Dinesh Prasad(dinesh@malhar-inc.com)
 */
public class RandomEvenetGeneratorSample implements StreamingApplication
{
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
		// Create application dag.
	    dag.setAttribute(DAG.APPLICATION_NAME, "MobileDevApplication");
	    dag.setAttribute(DAG.DEBUG, true);

	    // Add random integer generator operator
	    RandomEventGenerator rand = dag.addOperator("rand", RandomEventGenerator.class);
	    rand.setMaxvalue(999999999);
	    rand.setTuplesBlast(10);
	    rand.setTuplesBlastIntervalMillis(1000);

	    // Connect to output console operator
	    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
	    dag.addStream("rand_console",rand.integer_data , console.input);

	    // done
	}


}
