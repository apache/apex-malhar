/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.samplestream;

import au.com.bytecode.opencsv.CSVReader;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.lib.io.SimpleSinglePortInputOperator;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.DefaultHttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class YahooFinanceCSVSpout extends SimpleSinglePortInputOperator<ArrayList<String>> implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(YahooFinanceCSVSpout.class);
  /**
   * Timeout interval for reading from server. 0 or negative indicates no timeout.
   */
  public int readTimeoutMillis = 0;
  /**
   * The URL of the web service resource for the POST request.
   */
  @NotNull
  private String url;
  private transient HttpClient client;
  private transient GetMethod method;
  public static final String Symbol = "s";
  public static final String LastTrade = "l1";
  public static final String LastTradeDate = "d1";
  public static final String LastTradeTime = "t1";
  public static final String Change = "c1";
  public static final String Open = "o";
  public static final String DaysHigh = "h";
  public static final String DaysLow = "g";
  public static final String Volume = "v";
  private ArrayList<String> symbolList = new ArrayList<String>();
  private ArrayList<String> formatList = new ArrayList<String>();

  public void setUrl(String url)
  {
    this.url = url;
  }

  public void addSymbol(String symbol)
  {
    symbolList.add(symbol);
  }

  public void addFormat(String format)
  {
    formatList.add(format);
  }

  public ArrayList<String> getSymbolList()
  {
    return symbolList;
  }

  public ArrayList<String> getFormatList()
  {
    return formatList;
  }

  @Override
  public void setup(OperatorContext context)
  {
    client = new HttpClient();
    method = new GetMethod(url);
    DefaultHttpParams.getDefaultParams().setParameter("http.protocol.cookie-policy", CookiePolicy.BROWSER_COMPATIBILITY);

  }

  @Override
  public void teardown()
  {
    super.teardown();
  }

  @Override
  public void run()
  {
    while (true) {
      try {
        int statusCode = client.executeMethod(method);
        if (statusCode != HttpStatus.SC_OK) {
          System.err.println("Method failed: " + method.getStatusLine());
        }
        else {
          InputStream istream ;
          istream = method.getResponseBodyAsStream();
          // Process response
          InputStreamReader isr = new InputStreamReader(istream);
          CSVReader reader = new CSVReader(isr);
          List<String[]> myEntries;
          myEntries = reader.readAll();
          for (String[] stringArr : myEntries) {
            ArrayList<String> al = new ArrayList(Arrays.asList(stringArr));
            outputPort.emit(al);
          }
        }
        Thread.sleep(500);
      }
      catch (InterruptedException ex) {
        logger.debug(ex.toString());
      }
      catch (IOException ex) {
        logger.debug(ex.toString());
      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }
}
