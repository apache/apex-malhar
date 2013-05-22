/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.contrib.sqlite.SqliteStreamOperator;
import com.malhartech.lib.io.ConsoleOutputOperator;

import org.apache.hadoop.conf.Configuration;

/**
 * This demo will output the stock market data from yahoo finance
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class ApplicationWithSQL implements ApplicationFactory
{
  @Override
  public void getApplication(DAG dag, Configuration conf) {
    String[] symbols = {"YHOO","GOOG","AAPL","FB","AMZN","NFLX","IBM"};

    YahooFinanceCSVInputOperator input1 = dag.addOperator("input1", new YahooFinanceCSVInputOperator());
    YahooFinanceCSVInputOperator input2 = dag.addOperator("input2", new YahooFinanceCSVInputOperator());
    SqliteStreamOperator sqlOper = dag.addOperator("sqlOper", new SqliteStreamOperator());
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

    SqliteStreamOperator.InputSchema inputSchema1 = new SqliteStreamOperator.InputSchema("t1");
    SqliteStreamOperator.InputSchema inputSchema2 = new SqliteStreamOperator.InputSchema("t2");
    inputSchema1.setColumnInfo("s0", "string", true); // symbol
    inputSchema1.setColumnInfo("l1", "float", false);  // last trade
    inputSchema2.setColumnInfo("s0", "string", true); // symbol
    inputSchema2.setColumnInfo("e0", "float", false);  // EPS
    inputSchema2.setColumnInfo("b4", "float", false);  // Book value

    sqlOper.setInputSchema(0, inputSchema1);
    sqlOper.setInputSchema(1, inputSchema2);

    // Calculate PE Ratio and PB Ratio using SQL
    sqlOper.setStatement("SELECT t1.s0 AS symbol, t1.l1 / t2.e0 AS pe_ratio, t1.l1 / t2.b4 AS pb_ratio FROM t1,t2 WHERE t1.s0 = t2.s0");

    dag.addStream("input1_sql", input1.outputPort, sqlOper.in1);
    dag.addStream("input2_sql", input2.outputPort, sqlOper.in2);

    dag.addStream("result_console", sqlOper.result, consoleOperator.input);

  }
}
