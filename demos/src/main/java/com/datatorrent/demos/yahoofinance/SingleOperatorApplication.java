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
package com.datatorrent.demos.yahoofinance;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.SmtpOutputOperator;
import com.datatorrent.lib.streamquery.DerbySqlStreamOperator;
import com.datatorrent.lib.util.AlertEscalationOperator;

import org.apache.hadoop.conf.Configuration;

/**
 * This demo will output the stock market data from yahoo finance
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name="YahooFinanceApplicationWithSingleOperator")
public class SingleOperatorApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String[] symbols = {"YHOO", "GOOG", "AAPL", "FB", "AMZN", "NFLX", "IBM"};

    YahooFinanceCSVInputOperator yahooFinance = dag.addOperator("yahooFinance", new YahooFinanceCSVInputOperator());

    for (String symbol: symbols) {
      yahooFinance.addSymbol(symbol);
    }
    yahooFinance.addFormat("s0");
    yahooFinance.addFormat("l1");

  }

}
