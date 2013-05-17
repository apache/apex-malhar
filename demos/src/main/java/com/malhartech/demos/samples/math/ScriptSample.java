package com.malhartech.demos.samples.math;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.*;
import com.malhartech.lib.script.JavaScriptOperator;
import com.malhartech.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

/**  
 * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> Script <br>
 * <bClass : </b> com.malhartech.lib.math.Script 
 * Java script returns square of variable 'val'.
 * 
 * @author Dinesh Prasad(dinesh@malhar-inc.com)
 */
public class ScriptSample implements ApplicationFactory 
{
	@Override
	public DAG getApplication(Configuration conf) 
	{
		// Create application dag.
	    DAG dag = new DAG(conf);
	    dag.setAttribute(DAG.STRAM_APPNAME, "MobileDevApplication");
	    dag.setAttribute(DAG.STRAM_DEBUG, true);
	    
	    // Add random integer generator operator
	    SingleKeyValMap rand = dag.addOperator("rand", SingleKeyValMap.class);
	    
	    JavaScriptOperator script = dag.addOperator("script", JavaScriptOperator.class);
	    //script.setEval("val = val*val;");
	    script.addSetupScript("function square() { return val*val;}");
	    script.setInvoke("square");
	    dag.addStream("evalstream", rand.outport, script.inBindings);
	    
	    // Connect to output console operator
	    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
	    dag.addStream("rand_console",script.result , console.input);
	    
	    // done 
		return dag;
	}
	
	
}
