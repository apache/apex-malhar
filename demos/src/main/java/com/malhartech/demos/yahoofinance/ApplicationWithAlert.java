/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.contrib.sqlite.SqliteStreamOperator;
import com.malhartech.lib.io.SmtpOutputOperator;
import com.malhartech.lib.util.Alert;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;

/**
 * This demo will output the stock market data from yahoo finance
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class ApplicationWithAlert implements ApplicationFactory
{
  @Override
  public DAG getApplication(Configuration conf)
  {
    String[] symbols = {"YHOO", "GOOG", "AAPL", "FB", "AMZN", "NFLX", "IBM"};
    DAG dag = new DAG();

    YahooFinanceCSVInputOperator input1 = dag.addOperator("input1", new YahooFinanceCSVInputOperator());
    SqliteStreamOperator sqlOper = dag.addOperator("sqlOper", new SqliteStreamOperator());
    Alert<HashMap<String, Object>> alertOper = dag.addOperator("alert", new Alert<HashMap<String, Object>>());
    //ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    SmtpOutputOperator<HashMap<String, Object>> mailOper = dag.addOperator("mail", new SmtpOutputOperator<HashMap<String, Object>>());

    mailOper.setFrom("jenkins@malhar-inc.com");
    mailOper.addRecipient(SmtpOutputOperator.RecipientType.TO, "jenkins@malhar-inc.com");
    mailOper.setContent("AAPL: {}\nThis is an auto-generated message. Do not reply.");
    mailOper.setSubject("ALERT: AAPL is less than 450");
    mailOper.setSmtpHost("secure.emailsrvr.com");
    mailOper.setSmtpPort(465);
    mailOper.setSmtpUserName("jenkins@malhar-inc.com");
    mailOper.setSmtpPassword("Testing1");
    mailOper.setUseSsl(true);

    alertOper.setAlertFrequency(60000); // 60 seconds

    for (String symbol: symbols) {
      input1.addSymbol(symbol);
    }
    input1.addFormat("s0");
    input1.addFormat("l1");

    SqliteStreamOperator.InputSchema inputSchema1 = new SqliteStreamOperator.InputSchema("t1");
    inputSchema1.setColumnInfo("s0", "string", true); // symbol
    inputSchema1.setColumnInfo("l1", "float", false);  // last trade

    sqlOper.setInputSchema(0, inputSchema1);

    // select the alert using SQL
    sqlOper.setStatement("SELECT t1.s0 AS symbol, t1.l1 AS last_trade FROM t1 WHERE t1.s0 = 'AAPL' AND t1.l1 < 450");

    dag.addStream("input1_sql", input1.outputPort, sqlOper.in1);
    dag.addStream("sql_alert", sqlOper.result, alertOper.in);
    //dag.addStream("alert_console", alertOper.alert1, consoleOperator.input);
    dag.addStream("alert_mail", alertOper.alert1, mailOper.input);

    return dag;
  }

}
