/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.apachelog;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.contrib.zmq.SimpleSinglePortZeroMQPullStringInputOperator;
import com.malhartech.lib.algo.UniqueCounter;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.logs.ApacheLogParseOperator;
import com.malhartech.lib.math.Sum;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class Application implements ApplicationFactory
{
  private boolean allInline = false;
  private final String addr = "tcp://127.0.0.1:5555";

  @Override
  public DAG getApplication(Configuration conf)
  {
    allInline = true;

    DAG dag = new DAG(conf);

    dag.getAttributes().attr(DAG.STRAM_WINDOW_SIZE_MILLIS).set(1000);
    SimpleSinglePortZeroMQPullStringInputOperator input = dag.addOperator("input", new SimpleSinglePortZeroMQPullStringInputOperator(addr));
    ApacheLogParseOperator parse = dag.addOperator("parse", new ApacheLogParseOperator());
    UniqueCounter<String> ipAddrCount = dag.addOperator("ipAddrCount", new UniqueCounter<String>());
    UniqueCounter<String> urlCount = dag.addOperator("urlCount", new UniqueCounter<String>());
    UniqueCounter<String> httpStatusCount = dag.addOperator("httpStatusCount", new UniqueCounter<String>());
    Sum<Long> numOfBytesSum = dag.addOperator("numOfBytesSum", new Sum<Long>());
    //ArrayListAggregator<Long> agg = dag.addOperator("agg", new ArrayListAggregator<Long>());

    //dag.getOperatorWrapper(agg).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(3);
    dag.getOperatorMeta(numOfBytesSum).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(3);

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

    return dag;
  }

}
