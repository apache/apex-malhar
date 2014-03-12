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
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.SmtpOutputOperator;
import com.datatorrent.lib.streamquery.DerbySqlStreamOperator;

import org.apache.hadoop.conf.Configuration;

/**
 * This demo will output the stock market data from yahoo finance
 *
 * @since 0.3.2
 */
public class ApplicationWithAlert implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String symbolStr = conf.get(ApplicationWithAlert.class.getName() + ".tickerSymbols", "YHOO,GOOG,AAPL,FB,AMZN,NFLX,IBM");
    String thresholdStr = conf.get(ApplicationWithAlert.class.getName() + ".tickerThreshold", "39.00, 1100.00, 540.00, 73.00, 365.00, 448.00, 185.00");

    String[] symbols = symbolStr.split(",");
    String[] priceThreshold = thresholdStr.split(",");
    Double[] tickerThreshold = new Double[priceThreshold.length];

    for (int i=0; i<priceThreshold.length; i++){
        tickerThreshold[i] = Double.parseDouble(priceThreshold[i]);
    }

    YahooFinanceCSVInputOperator input1 = dag.addOperator("input1", new YahooFinanceCSVInputOperator());
    DerbySqlStreamOperator sqlOper = dag.addOperator("sqlOper", new DerbySqlStreamOperator());
    YahooFinanceAlertEscalationOperator alertOper = dag.addOperator("alert", new YahooFinanceAlertEscalationOperator());

    Integer alertInterval = Integer.valueOf(conf.get(ApplicationWithAlert.class.getName() + ".alertInterval", "60000"));
    alertOper.setAlertInterval(alertInterval); // 60 seconds


    for (String symbol: symbols) {
      input1.addSymbol(symbol);
    }
    input1.addFormat("s0");
    input1.addFormat("l1");

    DerbySqlStreamOperator.InputSchema inputSchema1 = new DerbySqlStreamOperator.InputSchema("t1");
    inputSchema1.setColumnInfo("s0", "varchar(100)", true); // symbol
    inputSchema1.setColumnInfo("l1", "float", false);  // last trade

    sqlOper.setInputSchema(0, inputSchema1);

    // Generate the alert for each symbols.
    Integer i = 0;
    for (String smbol: symbols)  {
        sqlOper.addExecStatementString("SELECT SESSION.t1.s0 AS symbol, SESSION.t1.l1 AS last_trade FROM SESSION.t1 WHERE SESSION.t1.s0 = '" + smbol + "' AND SESSION.t1.l1 < " + tickerThreshold[i]);
        i++;
    }

//    sqlOper.setStatement("SELECT SESSION.t1.s0 AS symbol, SESSION.t1.l1 AS last_trade FROM SESSION.t1 WHERE SESSION.t1.s0 = 'AAPL' AND SESSION.t1.l1 < 450");

    dag.addStream("input1_sql", input1.outputPort, sqlOper.in1);
    dag.addStream("sql_alert", sqlOper.result, alertOper.in);

      boolean isSmtp = validateSmtpParams(conf);

      if (isSmtp) {
          SmtpOutputOperator mailOper = dag.addOperator("mail", getSMTPOperator(conf));
          dag.addStream("alert_mail", alertOper.alert, mailOper.input);
      } else {
          ConsoleOutputOperator consoleOperator = new ConsoleOutputOperator();
          dag.addOperator("console", consoleOperator);
          dag.addStream("alert_console", alertOper.alert, consoleOperator.input);
      }

  }

    private boolean validateSmtpParams(Configuration conf) {
        String From = conf.get(ApplicationWithAlert.class.getName() + ".From");
        String To = conf.get(ApplicationWithAlert.class.getName() + ".To");
        String smtpHost = conf.get(ApplicationWithAlert.class.getName() + ".smtpHost");
        String smtpPort = conf.get(ApplicationWithAlert.class.getName() + ".smtpPort");
        String smtpUser = conf.get(ApplicationWithAlert.class.getName() + ".smtpUser");
        String smtpPasswd = conf.get(ApplicationWithAlert.class.getName() + ".smtpPasswd");
        return (isNotNull(From) && isNotNull(To) && isNotNull(smtpHost) && isNotNull(smtpPort) && isNotNull(smtpUser) && isNotNull(smtpPasswd));
    }

    private SmtpOutputOperator getSMTPOperator(Configuration conf) {
        String From = conf.get(ApplicationWithAlert.class.getName() + ".From");
        String To = conf.get(ApplicationWithAlert.class.getName() + ".To");
        String smtpHost = conf.get(ApplicationWithAlert.class.getName() + ".smtpHost");
        Integer smtpPort = Integer.parseInt(conf.get(ApplicationWithAlert.class.getName() + ".smtpPort"));
        String smtpUser = conf.get(ApplicationWithAlert.class.getName() + ".smtpUser");
        String smtpPasswd = conf.get(ApplicationWithAlert.class.getName() + ".smtpPasswd");
        Boolean useSsl = Boolean.valueOf(conf.get(ApplicationWithAlert.class.getName() + ".useSsl"));

        SmtpOutputOperator mailOper = new SmtpOutputOperator();

        mailOper.setFrom(From);
        mailOper.addRecipient(SmtpOutputOperator.RecipientType.TO, To);
        mailOper.setContent("Threshold breach: {}\nThis is an auto-generated message. Do not reply.");
        mailOper.setSubject("ALERT: Price drops below threshold");
        mailOper.setSmtpHost(smtpHost);
        mailOper.setSmtpPort(smtpPort);
        mailOper.setSmtpUserName(smtpUser);
        mailOper.setSmtpPassword(smtpPasswd);
        mailOper.setUseSsl(useSsl);

        return mailOper;
    }

    private boolean isNotNull(String str) {
        return (str != null && str.length() > 0);
    }

}
