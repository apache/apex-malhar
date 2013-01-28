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
 * This demo will output the stock market data from yahoo finance
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
    YahooFinanceCSVInputOperator spout = dag.addOperator("spout", new YahooFinanceCSVInputOperator());
    spout.addSymbol("GOOG");
    spout.addSymbol("FB");
    spout.addSymbol("YHOO");
    spout.addFormat(YahooFinanceCSVInputOperator.Symbol);
    spout.addFormat(YahooFinanceCSVInputOperator.LastTrade);
    spout.addFormat(YahooFinanceCSVInputOperator.LastTradeDate);
    spout.addFormat(YahooFinanceCSVInputOperator.LastTradeTime);
    spout.addFormat(YahooFinanceCSVInputOperator.Change);
    spout.addFormat(YahooFinanceCSVInputOperator.Open);
    spout.addFormat(YahooFinanceCSVInputOperator.DaysHigh);
    spout.addFormat(YahooFinanceCSVInputOperator.DaysLow);
    spout.addFormat(YahooFinanceCSVInputOperator.Volume);

    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("spout-console", spout.outputPort, consoleOperator.input).setInline(allInline);

    return dag;
  }
}
