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
package com.datatorrent.contrib.apachelog;

import com.datatorrent.contrib.zmq.SimpleSinglePortZeroMQPullStringInputOperator;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.logs.ApacheLogParseOperator;
import com.datatorrent.lib.math.Sum;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import org.apache.hadoop.conf.Configuration;

/**
 * <p>Application class.</p>
 *
 * @since 0.3.2
 */
public class Application implements StreamingApplication
{
  private boolean allInline = false;
  private final String addr = "tcp://127.0.0.1:5555";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    allInline = true;

    dag.getAttributes().attr(DAG.STREAMING_WINDOW_SIZE_MILLIS).set(1000);
    SimpleSinglePortZeroMQPullStringInputOperator input = dag.addOperator("input", new SimpleSinglePortZeroMQPullStringInputOperator(addr));
    ApacheLogParseOperator parse = dag.addOperator("parse", new ApacheLogParseOperator());
    UniqueCounter<String> ipAddrCount = dag.addOperator("ipAddrCount", new UniqueCounter<String>());
    UniqueCounter<String> urlCount = dag.addOperator("urlCount", new UniqueCounter<String>());
    UniqueCounter<String> httpStatusCount = dag.addOperator("httpStatusCount", new UniqueCounter<String>());
    Sum<Long> numOfBytesSum = dag.addOperator("numOfBytesSum", new Sum<Long>());
    //ArrayListAggregator<Long> agg = dag.addOperator("agg", new ArrayListAggregator<Long>());

    //dag.getOperatorWrapper(agg).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(3);
    dag.getMeta(numOfBytesSum).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(3);

    dag.addStream("input-parse", input.outputPort, parse.data).setInline(allInline);
    dag.addStream("parse-ipAddrCount", parse.outputIPAddress, ipAddrCount.data).setInline(allInline);
    dag.addStream("parse-urlCount", parse.outputUrl, urlCount.data).setInline(allInline);
    dag.addStream("parse-httpStatusCount", parse.outputStatusCode, httpStatusCount.data).setInline(allInline);
    dag.addStream("parse-numOfBytesSum", parse.outputBytes, numOfBytesSum.data).setInline(allInline);
    //dag.addStream("numOfBytesSum-agg", numOfBytesSum.sumLong, agg.input);

    ConsoleOutputOperator consoleOperator1 = dag.addOperator("console1", new ConsoleOutputOperator());
    ConsoleOutputOperator consoleOperator2 = dag.addOperator("console2", new ConsoleOutputOperator());
    ConsoleOutputOperator consoleOperator3 = dag.addOperator("console3", new ConsoleOutputOperator());
    ConsoleOutputOperator consoleOperator4 = dag.addOperator("console4", new ConsoleOutputOperator());

    dag.addStream("ipAddrCount-console", ipAddrCount.count, consoleOperator1.input);
    dag.addStream("urlCount-console", urlCount.count, consoleOperator2.input);
    dag.addStream("httpStatusCount-console", httpStatusCount.count, consoleOperator3.input);
    //dag.addStream("agg-console", agg.output, consoleOperator4.input);
    dag.addStream("numOfBytesSum-console", numOfBytesSum.sumLong, consoleOperator4.input);

  }

}
