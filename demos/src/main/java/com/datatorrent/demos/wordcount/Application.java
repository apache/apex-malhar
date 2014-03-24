/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.wordcount;

import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import org.apache.hadoop.conf.Configuration;

/**
 * Simple Word Count Demo : <br>
 * This is application to count total occurrence of each word from file or any
 * stream. <br>
 * <br>
 *
 * Functional Description : <br>
 * This demo declares custom input operator to read data file set by user. <br>
 * This input operator can be replaced by any stream input operator. <br>
 * <br>
 *
 * Custom Attribute(s) : None <br>
 * <br>
 *
 * Input Adapter : <br>
 * Word input operator opens user specified data file and streams each line to
 * application. <br>
 * <br>
 *
 * Output Adapter : <br>
 * Output values are written to console through ConsoleOutputOerator<br>
 * If needed you can use other output adapters<br>
 * <br>
 * <p>
 * Running Java Test or Main app in IDE:
 *
 * <pre>
 *     LocalMode.runApp(new Application(), 600000); // 10 min run
 * </pre>
 *
 * Run Success : <br>
 * For successful deployment and run, user should see following output on console: <br>
 * </pre>
 * {developed=1} {bush’s=2} {roster=1} {council=1} {mankiw=1} {academia=1}
 * {of=6} {help=1} {are=1} {presidential=1}
 * </pre> <br> <br>
 *
 * Scaling Options : <br>
 * This operator app can not be scaled, please look at implementation {@link com.datatorrent.lib.algo.UniqueCounterEach}  <br> <br>
 *
 * Application DAG : <br>
 * <img src="doc-files/UniqueWordCounter.jpg" width=600px > <br>
 *
 * Streaming Window Size : 500ms
 * Operator Details : <br>
 * <ul>
 * 	<li>
 *     <p><b> The operator wordinput : </b> This operator opens local file, reads each line and sends each word to application.
 *         This can replaced by any input stream by user. <br>
 *     Class : {@link com.datatorrent.demos.wordcount.WordCountInputOperator}  <br>
 *     Operator Application Window Count : 1 <br>
 *     StateFull : No
 *  </li>
 *  <li>
 *     <p><b> The operator count : </b>  This operator aggregates unique key count  over one window count(app). <br>
 *     Class : {@link com.datatorrent.lib.algo.UniqueCounterEach}  <br>
 *     Operator Application Window Count : 1 <br>
 *     StateFull : No
 *  </li>
 *  <li>
 *      <p><b>The operator Console: </b> This operator just outputs the input tuples to  the console (or stdout).
 *      This case it emits unique count of each word over 500ms.
 *  </li>
 * </ul>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name="WordCountApplication")
public class Application implements StreamingApplication
{
  protected String fileName = "com/datatorrent/demos/wordcount/samplefile.txt";
  private Locality locality = null;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    locality = Locality.CONTAINER_LOCAL;

    WordCountInputOperator input = dag.addOperator("wordinput", new WordCountInputOperator());
    input.setFileName(fileName);
    UniqueCounter<String> wordCount = dag.addOperator("count", new UniqueCounter<String>());

    dag.addStream("wordinput-count", input.outputPort, wordCount.data).setLocality(locality);

    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("count-console",wordCount.count, consoleOperator.input);

  }


}
