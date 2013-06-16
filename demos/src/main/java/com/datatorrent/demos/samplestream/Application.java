/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.samplestream;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.LoggerFactory;

/**
 * This demo will output the stock market data from yahoo finance
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class Application implements StreamingApplication
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Application.class);
  private final boolean allInline = false;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    YahooFinanceCSVInputOperator input = dag.addOperator("input", new YahooFinanceCSVInputOperator());
    input.addSymbol("GOOG");
    input.addSymbol("FB");
    input.addSymbol("YHOO");
    input.addFormat(YahooFinanceCSVInputOperator.Symbol);
    input.addFormat(YahooFinanceCSVInputOperator.LastTrade);
    input.addFormat(YahooFinanceCSVInputOperator.LastTradeDate);
    input.addFormat(YahooFinanceCSVInputOperator.LastTradeTime);
    input.addFormat(YahooFinanceCSVInputOperator.Change);
    input.addFormat(YahooFinanceCSVInputOperator.Open);
    input.addFormat(YahooFinanceCSVInputOperator.DaysHigh);
    input.addFormat(YahooFinanceCSVInputOperator.DaysLow);
    input.addFormat(YahooFinanceCSVInputOperator.Volume);

    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("input-console", input.outputPort, consoleOperator.input).setInline(allInline);

  }
}
