/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.samplestream;

import au.com.bytecode.opencsv.CSVReader;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.SimpleSinglePortInputOperator;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.DefaultHttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Get stock information from Yahoo finance site. <p>
 *
 * @since 0.3.2
 */
public class YahooFinanceCSVInputOperator extends SimpleSinglePortInputOperator<ArrayList<String>> implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(YahooFinanceCSVInputOperator.class);
  /**
   * Timeout interval for reading from server. 0 or negative indicates no timeout.
   */
  private int readIntervalMillis = 500;

  /**
   * The URL of the web service resource for the POST request.
   */
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
  private ArrayList<String> parameterList = new ArrayList<String>();

  public void addSymbol(String symbol)
  {
    symbolList.add(symbol);
  }

  public void addFormat(String format)
  {
    parameterList.add(format);
  }

  public ArrayList<String> getSymbolList()
  {
    return symbolList;
  }

  public ArrayList<String> getParameterList()
  {
    return parameterList;
  }

  public int getReadIntervalMillis()
  {
    return readIntervalMillis;
  }

  public void setReadIntervalMillis(int readIntervalMillis)
  {
    this.readIntervalMillis = readIntervalMillis;
  }

  /**
   * Prepare URL from symbols and parameters.
   * URL will be something like: http://download.finance.yahoo.com/d/quotes.csv?s=GOOG,FB,YHOO&f=sl1vt1&e=.csv
   * @return
   */
  private String prepareURL()
  {
    String str = "http://download.finance.yahoo.com/d/quotes.csv?";

    str += "s=";
    for (int i = 0; i < symbolList.size(); i++) {
      if (i != 0) {
        str += ",";
      }
       str += symbolList.get(i);
    }
    str += "&f=";
    for (String format: parameterList) {
      str += format;
    }
    str += "&e=.csv";
    return str;
  }

  @Override
  public void setup(OperatorContext context)
  {
    url = prepareURL();
    client = new HttpClient();
    method = new GetMethod(url);
    DefaultHttpParams.getDefaultParams().setParameter("http.protocol.cookie-policy", CookiePolicy.BROWSER_COMPATIBILITY);
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
          InputStream istream;
          istream = method.getResponseBodyAsStream();
          // Process response
          InputStreamReader isr = new InputStreamReader(istream);
          CSVReader reader = new CSVReader(isr);
          List<String[]> myEntries;
          myEntries = reader.readAll();
          for (String[] stringArr: myEntries) {
            ArrayList<String> al = new ArrayList<String>(Arrays.asList(stringArr));
            outputPort.emit(al); // send out one symbol at a time
          }
        }
        Thread.sleep(readIntervalMillis);
      }
      catch (InterruptedException ex) {
        logger.debug(ex.toString());
      }
      catch (IOException ex) {
        logger.debug(ex.toString());
      }
    }
  }
}
