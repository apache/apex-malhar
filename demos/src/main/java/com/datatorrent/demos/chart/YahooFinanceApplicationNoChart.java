/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.chart;

import com.datatorrent.demos.yahoofinance.StockTickInput;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.malhartech.api.DAG;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class YahooFinanceApplicationNoChart extends YahooFinanceApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.getAttributes().attr(DAG.STREAMING_WINDOW_SIZE_MILLIS).set(streamingWindowSizeMilliSeconds);

    StockTickInput tick = getStockTickInputOperator("StockTickInput", dag);
    tick.setOutputEvenIfZeroVolume(true);
    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("price", tick.price, consoleOperator.input);

  }
}
