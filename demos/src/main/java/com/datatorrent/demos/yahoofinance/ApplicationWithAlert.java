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
@ApplicationAnnotation(name="YahooFinanceWithAlert")
public class ApplicationWithAlert implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String[] symbols = {"YHOO", "GOOG", "AAPL", "FB", "AMZN", "NFLX", "IBM"};

    YahooFinanceCSVInputOperator input1 = dag.addOperator("input1", new YahooFinanceCSVInputOperator());
    DerbySqlStreamOperator sqlOper = dag.addOperator("sqlOper", new DerbySqlStreamOperator());
    AlertEscalationOperator alertOper = dag.addOperator("alert", new AlertEscalationOperator());
    //ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    SmtpOutputOperator mailOper = dag.addOperator("mail", new SmtpOutputOperator());

    mailOper.setFrom("jenkins@datatorrent.com");
    mailOper.addRecipient(SmtpOutputOperator.RecipientType.TO, "jenkins@datatorrent.com");
    mailOper.setContent("AAPL: {}\nThis is an auto-generated message. Do not reply.");
    mailOper.setSubject("ALERT: AAPL is less than 450");
    mailOper.setSmtpHost("secure.emailsrvr.com");
    mailOper.setSmtpPort(465);
    mailOper.setSmtpUserName("jenkins@datatorrent.com");
    mailOper.setSmtpPassword("Testing1");
    mailOper.setUseSsl(true);

    alertOper.setAlertInterval(60000); // 60 seconds

    for (String symbol: symbols) {
      input1.addSymbol(symbol);
    }
    input1.addFormat("s0");
    input1.addFormat("l1");

    DerbySqlStreamOperator.InputSchema inputSchema1 = new DerbySqlStreamOperator.InputSchema("t1");
    inputSchema1.setColumnInfo("s0", "varchar(100)", true); // symbol
    inputSchema1.setColumnInfo("l1", "float", false);  // last trade

    sqlOper.setInputSchema(0, inputSchema1);

    // select the alert using SQL
    sqlOper.setStatement("SELECT SESSION.t1.s0 AS symbol, SESSION.t1.l1 AS last_trade FROM SESSION.t1 WHERE SESSION.t1.s0 = 'AAPL' AND SESSION.t1.l1 < 450");

    dag.addStream("input1_sql", input1.outputPort, sqlOper.in1);
    dag.addStream("sql_alert", sqlOper.result, alertOper.in);
    //dag.addStream("alert_console", alertOper.alert1, consoleOperator.input);
    dag.addStream("alert_mail", alertOper.alert, mailOper.input);

  }

}
