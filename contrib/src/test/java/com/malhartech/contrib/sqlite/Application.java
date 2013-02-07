/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.sqlite;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.LoggerFactory;

/**
 * This demo will output the stock market data from yahoo finance
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class Application implements ApplicationFactory
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Application.class);
  private boolean allInline = false;

  @Override
  public DAG getApplication(Configuration conf) {
    String[] symbols = {"YHOO","GOOG","AAPL","FB","AMZN","NFLX","IBM"};
    DAG dag = new DAG();

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
    inputSchema1.setColumnType("s0", "string"); // symbol
    inputSchema1.setColumnType("l1", "float");  // last trade
    inputSchema2.setColumnType("s0", "string"); // symbol
    inputSchema2.setColumnType("e0", "float");  // EPS
    inputSchema2.setColumnType("b4", "float");  // Book value

    sqlOper.setInputSchema(0, inputSchema1);
    sqlOper.setInputSchema(1, inputSchema2);

    // Calculate PE Ratio and PB Ratio using SQL
    sqlOper.setStatement("SELECT t1.s0 AS symbol, t1.l1 / t2.e0 AS pe_ratio, t1.l1 / t2.b4 AS pb_ratio FROM t1,t2 WHERE t1.s0 = t2.s0");

    dag.addStream("input1_sql", input1.outputPort, sqlOper.in1);
    dag.addStream("input2_sql", input2.outputPort, sqlOper.in2);

    dag.addStream("result_console", sqlOper.result, consoleOperator.input).setInline(allInline);

    return dag;
  }
}
