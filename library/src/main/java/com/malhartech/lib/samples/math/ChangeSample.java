package com.malhartech.lib.samples.math;

import org.apache.hadoop.conf.Configuration;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.math.Change;
import com.malhartech.lib.testbench.RandomEventGenerator;

/**
 *  This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> Change <br>
 * <bClass : </b> com.malhartech.lib.math.Change
 *
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 *
 * @author Dinesh Prasad(dinesh@malhar-inc.com)
 */
public class ChangeSample implements ApplicationFactory
{
	@SuppressWarnings("unchecked")
	@Override
	public void getApplication(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAG.STRAM_APPNAME, "ChangeSample");
		dag.setAttribute(DAG.STRAM_DEBUG, true);

		// Add random integer generator operator
		RandomEventGenerator rand = dag.addOperator("rand",
				RandomEventGenerator.class);
		rand.setMaxvalue(999999999);
		rand.setTuplesBlast(10);
		rand.setTuplesBlastIntervalMillis(1000);
		RandomEventGenerator rand1 = dag.addOperator("rand1",
				RandomEventGenerator.class);
		rand.setTuplesBlast(1);
		rand.setTuplesBlastIntervalMillis(5000);

		// append change operator
		Change<Integer> change = dag.addOperator("change", Change.class);
		dag.addStream("stream", rand1.integer_data, change.base);
		dag.addStream("stream1", rand.integer_data, change.data);

		// Connect to output console operator
		ConsoleOutputOperator console = dag.addOperator("console",
				new ConsoleOutputOperator());
		dag.addStream("consoleout", change.percent, console.input);

		// done
	}

}
