package com.malhartech.lib.samples.io;

import org.apache.hadoop.conf.Configuration;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.io.LocalFsInputOperator;

/**
 * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> AverageKeyVal <br>
 * <bClass : </b> com.malhartech.lib.math.AverageKeyVal
 * This application reads local demo text file and relays text content to output console.
 *
 * @author Dinesh Prasad(dinesh@malhar-inc.com)
 */
public class LocalFsInputSample implements ApplicationFactory
{
	@Override
	public void getApplication(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAG.STRAM_APPNAME, "MobileDevApplication");
		dag.setAttribute(DAG.STRAM_DEBUG, true);

		// Add random integer generator operator
		LocalFsInputOperator reader = dag.addOperator("reader",
				LocalFsInputOperator.class);
		reader
				.setFilePath("src/main/resources/com/malhartech/demos/wordcount/samplefile.txt");
		reader.setSleepInterval(1000);

		// Connect to output console operator
		ConsoleOutputOperator console = dag.addOperator("console",
				new ConsoleOutputOperator());
		dag.addStream("outstream", reader.outport, console.input);

		// done
	}
}
