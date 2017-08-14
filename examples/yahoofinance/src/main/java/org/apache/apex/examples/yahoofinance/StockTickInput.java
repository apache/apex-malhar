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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.DefaultHttpParams;
import org.apache.hadoop.util.StringUtils;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import au.com.bytecode.opencsv.CSVReader;

/**
 * This operator sends price, volume and time into separate ports and calculates incremental volume.
 *
 * @since 0.3.2
 */
public class StockTickInput implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(StockTickInput.class);
  /**
   * Timeout interval for reading from server. 0 or negative indicates no timeout.
   */
  public int readIntervalMillis = 500;
  /**
   * The URL of the web service resource for the POST request.
   */
  private String url;
  private String[] symbols;
  @NotNull
  private String tickers;
  private transient HttpClient client;
  private transient GetMethod method;
  private HashMap<String, Long> lastVolume = new HashMap<String, Long>();
  private boolean outputEvenIfZeroVolume = false;
  /**
   * The output port to emit price.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<String, Double>> price = new DefaultOutputPort<KeyValPair<String, Double>>();
  /**
   * The output port to emit incremental volume.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<String, Long>> volume = new DefaultOutputPort<KeyValPair<String, Long>>();
  /**
   * The output port to emit last traded time.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<String, String>> time = new DefaultOutputPort<KeyValPair<String, String>>();

  /**
   * Prepare URL from symbols and parameters. URL will be something like: http://download.finance.yahoo.com/d/quotes.csv?s=IBM,GOOG,AAPL,AABA&f=sl1vt1
   *
   * @return the URL
   */
  private String prepareURL()
  {
    String str = "http://download.finance.yahoo.com/d/quotes.csv?s=";
    for (int i = 0; i < symbols.length; i++) {
      if (i != 0) {
        str += ",";
      }
      str += symbols[i];
    }
    str += "&f=sl1vt1&e=.csv";
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
  public void teardown()
  {
  }

  @Override
  public void emitTuples()
  {

    try {
      int statusCode = client.executeMethod(method);
      if (statusCode != HttpStatus.SC_OK) {
        logger.error("Method failed: " + method.getStatusLine());
      } else {
        InputStream istream = method.getResponseBodyAsStream();
        // Process response
        InputStreamReader isr = new InputStreamReader(istream);
        CSVReader reader = new CSVReader(isr);
        List<String[]> myEntries = reader.readAll();
        for (String[] stringArr: myEntries) {
          ArrayList<String> tuple = new ArrayList<String>(Arrays.asList(stringArr));
          if (tuple.size() != 4) {
            return;
          }
          // input csv is <Symbol>,<Price>,<Volume>,<Time>
          String symbol = tuple.get(0);
          double currentPrice = Double.valueOf(tuple.get(1));
          long currentVolume = Long.valueOf(tuple.get(2));
          String timeStamp = tuple.get(3);
          long vol = currentVolume;
          // Sends total volume in first tick, and incremental volume afterwards.
          if (lastVolume.containsKey(symbol)) {
            vol -= lastVolume.get(symbol);
          }

          if (vol > 0 || outputEvenIfZeroVolume) {
            price.emit(new KeyValPair<String, Double>(symbol, currentPrice));
            volume.emit(new KeyValPair<String, Long>(symbol, vol));
            time.emit(new KeyValPair<String, String>(symbol, timeStamp));
            lastVolume.put(symbol, currentVolume);
          }
        }
      }
      Thread.sleep(readIntervalMillis);
    } catch (InterruptedException ex) {
      logger.debug(ex.toString());
    } catch (IOException ex) {
      logger.debug(ex.toString());
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

  public void setOutputEvenIfZeroVolume(boolean outputEvenIfZeroVolume)
  {
    this.outputEvenIfZeroVolume = outputEvenIfZeroVolume;
  }

  public void setTickers(String tickers)
  {
    this.tickers = tickers;
    symbols = StringUtils.split(tickers, ',');
  }

  public String getTickers()
  {
    return tickers;
  }

}
