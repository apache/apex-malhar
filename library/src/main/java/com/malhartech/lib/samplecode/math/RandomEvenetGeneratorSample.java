package com.malhartech.lib.samplecode.math;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.*;
import com.malhartech.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

/**
 * <b> Usage Operator : </b> com.malhartech.lib.testbench.RandomEventGenerator <br>
 * This sample usage for predefined operator <b>RandomEventGenerator</b>. <br>
 * Random generator output is printed to output console(can be any downstream operator).
 *
 * @author Dinesh Prasad(dinesh@malhar-inc.com)
 */
public class RandomEvenetGeneratorSample implements ApplicationFactory
{
	@Override
	public void getApplication(DAG dag, Configuration conf)
	{
		// Create application dag.
	    dag.setAttribute(DAG.STRAM_APPNAME, "MobileDevApplication");
	    dag.setAttribute(DAG.STRAM_DEBUG, true);

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
