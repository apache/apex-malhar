/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.examples.yahoofinance;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.io.SimpleSinglePortInputOperator;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.DefaultHttpParams;

import com.datatorrent.api.Context.OperatorContext;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Grabs Yahoo Finance quotes data and emits HashMap, with key equals the format name (e.g. "s0") <p>
 *
 * @since 0.3.2
 */
public class YahooFinanceCSVInputOperator extends SimpleSinglePortInputOperator<HashMap<String, Object>> implements Runnable
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
   * URL will be something like: http://download.finance.yahoo.com/d/quotes.csv?s=GOOG,FB,AABA&f=sl1vt1&e=.csv
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
          logger.error("Method failed: " + method.getStatusLine());
        } else {
          InputStream istream;
          istream = method.getResponseBodyAsStream();
          // Process response
          InputStreamReader isr = new InputStreamReader(istream);
          CSVReader reader = new CSVReader(isr);
          List<String[]> myEntries;
          myEntries = reader.readAll();
          for (String[] stringArr: myEntries) {
            HashMap<String,Object> hm = new HashMap<String,Object>();
            for (int i = 0; i < parameterList.size(); i++) {
              hm.put(parameterList.get(i), stringArr[i]);
            }
            outputPort.emit(hm); // send out one symbol at a time
          }
        }
        Thread.sleep(readIntervalMillis);
      } catch (InterruptedException ex) {
        logger.debug(ex.toString());
      } catch (IOException ex) {
        logger.debug(ex.toString());
      }
    }
  }
}
