/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.examples.pi;

import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.script.JavaScriptOperator;
import org.apache.apex.malhar.lib.stream.RoundRobinHashMap;
import org.apache.apex.malhar.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Monte Carlo PI estimation example : <br>
 * This application computes value of PI using Monte Carlo pi estimation
 * formula. This example inputs formula using java script operator.
 *
 * Running Java Test or Main app in IDE:
 *
 * <pre>
 * LocalMode.runApp(new Application(), 600000); // 10 min run
 * </pre>
 *
 * Run Success : <br>
 * For successful deployment and run, user should see following output on
 * console:
 * <pre>
 * 2013-06-25 11:44:25,842 [container-2] DEBUG stram.StramChildAgent updateOperatorStatus - container-2 pendingDeploy []
 * 2013-06-25 11:44:25,929 [ServerHelper-1-1] DEBUG netlet.AbstractClient send - allocating new sendBuffer4Offers of size 16384 for Server.Subscriber{type=rrhm_calc/3.inBindings, mask=0, partitions=null}
 * 3.16
 * 3.15
 * 3.1616
 * 3.148
 * 3.1393846153846154
 * </pre>
 *
 *  * Application DAG : <br>
 * <img src="doc-files/ApplicationScript.gif" width=600px > <br>
 * <br>
 *
 * Streaming Window Size : 1000 ms(1 Sec) <br>
 * Operator Details : <br>
 * <ul>
 * <li><b>The rand Operator : </b> This operator generates random integer
 * between 0-30k. <br>
 * Class : {@link org.apache.apex.malhar.lib.testbench.RandomEventGenerator} <br>
 * StateFull : No</li>
 *  <li><b>The rrhm Operator : </b> This operator takes input from random generator
 *  creates tuples of (x,y) in round robin fashion. <br>
 * Class : {@link org.apache.apex.malhar.lib.stream.RandomEventGenerator} <br>
 * StateFull : Yes, tuple is emitted after (x, y) values have been aggregated.</li>
 * <li><b>The calc operator : </b> This is java script operator implementing <br>
 * Class : {@link org.apache.apex.malhar.lib.math.Script} <br>
 * StateFull : No</li>
 * <li><b>The operator Console: </b> This operator just outputs the input tuples
 * to the console (or stdout). User can use any output adapter.  <br>
 * .</li>
 * </ul>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name = "PiJavaScriptExample")
public class ApplicationWithScript implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    int maxValue = 30000;

    RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());
    rand.setMinvalue(0);
    rand.setMaxvalue(maxValue);

    RoundRobinHashMap<String,Object> rrhm = dag.addOperator("rrhm", new RoundRobinHashMap<String, Object>());
    rrhm.setKeys(new String[]{"x", "y"});

    JavaScriptOperator calc = dag.addOperator("picalc", new JavaScriptOperator());
    calc.setPassThru(false);
    calc.put("i",0);
    calc.put("count",0);
    calc.addSetupScript("function pi() { if (x*x+y*y <= " + maxValue * maxValue + ") { i++; } count++; return i / count * 4; }");

    calc.setInvoke("pi");

    dag.addStream("rand_rrhm", rand.integer_data, rrhm.data);
    dag.addStream("rrhm_calc", rrhm.map, calc.inBindings);

    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("rand_console",calc.result, console.input);

  }

}
