/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.storm.samplestream;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class PrintSampleStream implements ApplicationFactory
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(PrintSampleStream.class);
  private boolean allInline = false;

  @Override
  public DAG getApplication(Configuration conf)
  {
    return getYahooFinanceCSVApplication(conf);
  }

  public DAG getYahooFinanceCSVApplication(Configuration conf) {
    DAG dag = new DAG();
    YahooFinanceCSVSpout spout = dag.addOperator("spout", new YahooFinanceCSVSpout());
//    String url = "http://finance.yahoo.com/d/quotes.csv?s=XOM+BBDb.TO+JNJ+MSFT&f=snd1l1yr";
    String url = "http://download.finance.yahoo.com/d/quotes.csv?";
//    String url = "http://finance.yahoo.com/d/quotes.csv?";
    spout.addSymbol("GOOG");
    spout.addSymbol("IBM");
    spout.addSymbol("YHOO");
    url += "s=";
    for( int i=0; i<spout.getSymbolList().size(); i++ ) {
      if( i == 0 ) {
        url += spout.getSymbolList().get(i);
      }
      else {
        url += ",";
        url += spout.getSymbolList().get(i);
      }
    }
    url += "&f=";
    spout.addFormat(YahooFinanceCSVSpout.Symbol);
    spout.addFormat(YahooFinanceCSVSpout.LastTrade);
    spout.addFormat(YahooFinanceCSVSpout.LastTradeDate);
    spout.addFormat(YahooFinanceCSVSpout.LastTradeTime);
    spout.addFormat(YahooFinanceCSVSpout.Change);
    spout.addFormat(YahooFinanceCSVSpout.Open);
    spout.addFormat(YahooFinanceCSVSpout.DaysHigh);
    spout.addFormat(YahooFinanceCSVSpout.DaysLow);
    spout.addFormat(YahooFinanceCSVSpout.Volume);
    for( String format : spout.getFormatList() ) {
      url += format;
    }
    url += "&e=.csv";
    logger.debug("url:"+url);
    spout.setUrl(url);
    ConsoleOutputOperator<ArrayList<String>> consoleOperator = dag.addOperator("console", new ConsoleOutputOperator<ArrayList<String>>());
    dag.addStream("spout-console", spout.outputPort, consoleOperator.input).setInline(allInline);

    return dag;
  }
/*
  public DAG getYahooFinanceJSApplication(Configuration conf) {
    DAG dag = new DAG();
    YahooFinanceJSSpout spout = dag.addOperator("spout", new YahooFinanceJSSpout());
    String url = "http://streamerapi.finance.yahoo.com/streamer/1.0?s=FB"
            + "&k=c10,g00,h00,l10,p20,v00&callback=parent.yfs_u1f&mktmcb=parent.yfs_mktmcb&gencallback=parent.yfs_gencb";
    try {
      spout.setUrl(new URI(url));
    }
    catch (URISyntaxException ex) {
      logger.debug(ex.toString());
    }
    ConsoleOutputOperator<String> consoleOperator = dag.addOperator("console", new ConsoleOutputOperator<String>());
    dag.addStream("spout-console", spout.outputPort, consoleOperator.input).setInline(allInline);

    return dag;
  }

  public DAG getYahooFinanceYQLApplication(Configuration conf) {
    DAG dag = new DAG();
    YahooFinanceYQLSpout spout = dag.addOperator("spout", new YahooFinanceYQLSpout());
    String url = "http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20csv%20where%20"
            + "url%3D'http%3A%2F%2Fdownload.finance.yahoo.com%2Fd%2Fquotes.csv%3F"
            + "s%3DYHOO%2CGOOG%2CAAPL%2CMSFT%26f%3Dsl1d1t1c1ohgv%26e%3D.csv'%20and%20columns%3D'"
            + "symbol%2Cprice%2Cdate%2Ctime%2Cchange%2Ccol1%2Chigh%2Clow%2Ccol2'&diagnostics=true";
    spout.setUrl(url);
//    ConsoleOutputOperator<String> consoleOperator = dag.addOperator("console", new ConsoleOutputOperator<String>());
//    dag.addStream("spout-console", spout.outputPort, consoleOperator.input).setInline(allInline);

    return dag;
  }
  public DAG getTwitterApplication(Configuration conf) {
    String username = "whatisfacetwit";
    String pwd = "111222";
    DAG dag = new DAG();

    TwitterSampleSpout spout = dag.addOperator("spout", new TwitterSampleSpout(username, pwd));
    PrintBolt bolt = dag.addOperator("bolt", new PrintBolt());

    dag.addStream("spout-bolt", spout.output, bolt.input).setInline(allInline);
    return dag;
  }
*/
}
