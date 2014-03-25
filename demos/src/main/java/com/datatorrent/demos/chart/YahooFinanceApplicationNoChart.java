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
package com.datatorrent.demos.chart;


import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.demos.yahoofinance.StockTickInput;
import com.datatorrent.lib.io.ConsoleOutputOperator;

/**
 * <p>YahooFinanceApplicationNoChart class.</p>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name="YahooFinanceApplicationWithoutChart")
public class YahooFinanceApplicationNoChart extends YahooFinanceApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.getAttributes().put(DAG.STREAMING_WINDOW_SIZE_MILLIS, streamingWindowSizeMilliSeconds);

    StockTickInput tick = getStockTickInputOperator("StockTickInput", dag);
    tick.setOutputEvenIfZeroVolume(true);
    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("price", tick.price, consoleOperator.input);

  }
}
