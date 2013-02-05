/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;

import au.com.bytecode.opencsv.CSVReader;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.annotation.ShipContainingJars;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import com.malhartech.lib.util.KeyValPair;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.DefaultHttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This operator sends price, volume and time into separate ports and calculates incremental volume.
 */
@ShipContainingJars(classes = {au.com.bytecode.opencsv.CSVReader.class})
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
  public String[] symbols;
  private transient HttpClient client;
  private transient GetMethod method;
  private HashMap<String, Long> lastVolume = new HashMap<String, Long>();
  /**
   * The output port to emit price.
   */
  @OutputPortFieldAnnotation(name = "price", optional = true)
  public final transient DefaultOutputPort<KeyValPair<String, Double>> price = new DefaultOutputPort<KeyValPair<String, Double>>(this);
  /**
   * The output port to emit incremental volume.
   */
  @OutputPortFieldAnnotation(name = "volume", optional = true)
  public final transient DefaultOutputPort<KeyValPair<String, Long>> volume = new DefaultOutputPort<KeyValPair<String, Long>>(this);
  /**
   * The output port to emit last traded time.
   */
  @OutputPortFieldAnnotation(name = "time", optional = true)
  public final transient DefaultOutputPort<KeyValPair<String, String>> time = new DefaultOutputPort<KeyValPair<String, String>>(this);

  /**
   * Prepare URL from symbols and parameters. URL will be something like: http://download.finance.yahoo.com/d/quotes.csv?s=IBM,GOOG,AAPL,YHOO&f=sl1vt1
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
        System.err.println("Method failed: " + method.getStatusLine());
      }
      else {
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

          if (vol > 0) {
            price.emit(new KeyValPair<String, Double>(symbol, currentPrice));
            volume.emit(new KeyValPair<String, Long>(symbol, vol));
            time.emit(new KeyValPair<String, String>(symbol, timeStamp));
            lastVolume.put(symbol, currentVolume);
          }
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

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

}
