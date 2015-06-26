/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.samples.lib.math;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.*;
import com.datatorrent.lib.script.JavaScriptOperator;
import org.apache.hadoop.conf.Configuration;

/**
 * This sample application code for showing sample usage of malhar operator(s). <br>
 * <b>Operator : </b> Script <br>
 * <bClass : </b> com.datatorrent.lib.math.Script
 * Java script returns square of variable 'val'.
 *
 * @since 0.3.2
 */
public class ScriptSample implements StreamingApplication
{
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
		// Create application dag.
	    dag.setAttribute(DAG.APPLICATION_NAME, "MobileDevApplication");
	    dag.setAttribute(DAG.DEBUG, true);

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
	}


}
