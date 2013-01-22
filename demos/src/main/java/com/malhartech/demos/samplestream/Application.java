/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.samplestream;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class Application implements ApplicationFactory
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Application.class);
  private boolean allInline = false;

  @Override
  public DAG getApplication(Configuration conf)
  {
    return getYahooFinanceCSVApplication(conf);
  }

  public DAG getYahooFinanceCSVApplication(Configuration conf) {
    DAG dag = new DAG();
    YahooFinanceCSVSpout spout = dag.addOperator("spout", new YahooFinanceCSVSpout());
    spout.addSymbol("GOOG");
    spout.addSymbol("FB");
    spout.addSymbol("YHOO");
    spout.addFormat(YahooFinanceCSVSpout.Symbol);
    spout.addFormat(YahooFinanceCSVSpout.LastTrade);
    spout.addFormat(YahooFinanceCSVSpout.LastTradeDate);
    spout.addFormat(YahooFinanceCSVSpout.LastTradeTime);
    spout.addFormat(YahooFinanceCSVSpout.Change);
    spout.addFormat(YahooFinanceCSVSpout.Open);
    spout.addFormat(YahooFinanceCSVSpout.DaysHigh);
    spout.addFormat(YahooFinanceCSVSpout.DaysLow);
    spout.addFormat(YahooFinanceCSVSpout.Volume);

    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("spout-console", spout.outputPort, consoleOperator.input).setInline(allInline);

    return dag;
  }
}
