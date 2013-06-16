package com.datatorrent.lib.samplecode.io;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.HttpInputOperator;
import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.api.DAGContext;

/**
 * * This sample application code for showing sample usage of malhar
 * operator(s). <br>
 * <b>Operator : </b> HttpInputOperator <br>
 * <bClass : </b> com.datatorrent.lib.io.HttpInputOperator
 *
 * this application connects to yahoo news and relays raw content to output console.
 *
 * @author Dinesh Prasad(dinesh@malhar-inc.com)
 */
public class HttpInputOperatorSample implements ApplicationFactory
{
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
		// Create application dag.
		dag.setAttribute(DAGContext.APPLICATION_NAME, "MobileDevApplication");
		dag.setAttribute(DAGContext.DEBUG, true);

		// Add random integer generator operator
		HttpInputOperator reader = dag.addOperator("reader",
				HttpInputOperator.class);
		reader.setUrl(URI.create("http://news.yahoo.com"));
		reader.readTimeoutMillis = 10000;

		// Connect to output console operator
		ConsoleOutputOperator console = dag.addOperator("console",
				new ConsoleOutputOperator());
		dag.addStream("consoleout", reader.rawOutput, console.input);

		// done
	}

}
