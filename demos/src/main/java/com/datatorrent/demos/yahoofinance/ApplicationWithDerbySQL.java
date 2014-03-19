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
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.streamquery.AbstractSqlStreamOperator;
import com.datatorrent.lib.streamquery.DerbySqlStreamOperator;

import org.apache.hadoop.conf.Configuration;

/**
 * This demo will output the stock market data from yahoo finance
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name="YahooFinanceWithDerbySQL")
public class ApplicationWithDerbySQL implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf) {
    String[] symbols = {"YHOO","GOOG","AAPL","FB","AMZN","NFLX","IBM"};

    YahooFinanceCSVInputOperator input1 = dag.addOperator("input1", new YahooFinanceCSVInputOperator());
    YahooFinanceCSVInputOperator input2 = dag.addOperator("input2", new YahooFinanceCSVInputOperator());
    DerbySqlStreamOperator sqlOper = dag.addOperator("sqlOper", new DerbySqlStreamOperator());
    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());

    for (String symbol : symbols) {
      input1.addSymbol(symbol);
      input2.addSymbol(symbol);
    }
    input1.addFormat("s0");
    input1.addFormat("l1");
    input2.addFormat("s0");
    input2.addFormat("e0");
    input2.addFormat("b4");

    AbstractSqlStreamOperator.InputSchema inputSchema1 = new AbstractSqlStreamOperator.InputSchema("t1");
    AbstractSqlStreamOperator.InputSchema inputSchema2 = new AbstractSqlStreamOperator.InputSchema("t2");
    inputSchema1.setColumnInfo("s0", "varchar(100)", true); // symbol
    inputSchema1.setColumnInfo("l1", "float", false);  // last trade
    inputSchema2.setColumnInfo("s0", "varchar(100)", true); // symbol
    inputSchema2.setColumnInfo("e0", "float", false);  // EPS
    inputSchema2.setColumnInfo("b4", "float", false);  // Book value

    sqlOper.setInputSchema(0, inputSchema1);
    sqlOper.setInputSchema(1, inputSchema2);

    // Calculate PE Ratio and PB Ratio using SQL
    sqlOper.setStatement("SELECT SESSION.t1.s0 AS symbol, SESSION.t1.l1 / SESSION.t2.e0 AS pe_ratio, SESSION.t1.l1 / SESSION.t2.b4 AS pb_ratio FROM SESSION.t1,SESSION.t2 WHERE SESSION.t1.s0 = SESSION.t2.s0");

    dag.addStream("input1_sql", input1.outputPort, sqlOper.in1);
    dag.addStream("input2_sql", input2.outputPort, sqlOper.in2);

    dag.addStream("result_console", sqlOper.result, consoleOperator.input);

  }
}
