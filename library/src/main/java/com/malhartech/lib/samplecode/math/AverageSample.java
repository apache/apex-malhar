package com.malhartech.lib.samplecode.math;

import org.apache.hadoop.conf.Configuration;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.math.Average;
import com.malhartech.lib.testbench.RandomEventGenerator;

/**
 * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> Average <br>
 * <bClass : </b> com.malhartech.lib.math.Average
 *
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class AverageSample implements ApplicationFactory
{
	@SuppressWarnings("unchecked")
	@Override
	public void getApplication(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAG.STRAM_APPNAME, "AverageSample");
		dag.setAttribute(DAG.STRAM_DEBUG, true);

		// Add random integer generator operator
		RandomEventGenerator rand = dag.addOperator("rand",
				RandomEventGenerator.class);
		rand.setMaxvalue(1000);
		rand.setTuplesBlast(10);
		rand.setTuplesBlastIntervalMillis(1000);

		Average<Integer> average = dag.addOperator("average",
				Average.class);
		dag.addStream("stream1", rand.integer_data, average.data);
		dag.getMeta(average).getAttributes()
				.attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(20);

		// Connect to output console operator
		ConsoleOutputOperator console = dag.addOperator("console",
				new ConsoleOutputOperator());
		dag.addStream("stream2", average.average, console.input);

		// done
	}

}
