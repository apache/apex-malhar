/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;

import au.com.bytecode.opencsv.CSVReader;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import com.malhartech.lib.util.KeyValPair;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.DefaultHttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *   This sends price, volume and time into separate ports and calculates incremental volume.
 *
 *   @author Locknath Shil <locknath@malhar-inc.com>
 */
public class StockTickInput implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(StockTickInput.class);
  /**
   *     Timeout interval for reading from server. 0 or negative indicates no timeout.
   */
  private int readIntervalMillis = 500;
  /**
   *     The URL of the web service resource for the POST request.
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
  private HashMap<String, Long> lastVolume = new HashMap<String, Long>();
  private final Random random = new Random();
  private final transient SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS"); // new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  private boolean isDummy = false;
  private boolean logTime = true;

  public void addSymbol(String symbol)
  {
    symbolList.add(symbol);
  }

  public int getReadIntervalMillis()
  {
    return readIntervalMillis;
  }

  public void setReadIntervalMillis(int readIntervalMillis)
  {
    this.readIntervalMillis = readIntervalMillis;
  }

  public void setIsDummy(boolean isDummy)
  {
    this.isDummy = isDummy;
  }

  public void setLogTime(boolean logTime)
  {
    this.logTime = logTime;
  }

  /**
   *     Prepare URL from symbols and parameters.
   *     URL will be something like: http://download.finance.yahoo.com/d/quotes.csv?s=GOOG,FB,YHOO&f=sl1vt1&e=.csv
   *
   *     @return
   */
  private String prepareURL()
  {
    String str = "http://download.finance.yahoo.com/d/quotes.csv?";
    str += "s=";
    for (int i = 0; i < symbolList.size(); i++) {
      if (i == 0) {
        str += symbolList.get(i);
      }
      else {
        str += ",";
        str += symbolList.get(i);
      }
    }
    str += "&f=";
    str += Symbol + LastTrade + Volume + LastTradeTime;
    str += "&e=.csv";
    return str;
  }

  @Override
  public void setup(OperatorContext context)
  {
    if (!isDummy) {
      url = prepareURL();
      client = new HttpClient();
      method = new GetMethod(url);
      DefaultHttpParams.getDefaultParams().setParameter("http.protocol.cookie-policy", CookiePolicy.BROWSER_COMPATIBILITY);
    }
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void emitTuples()
  {
    if (isDummy) {
      emitDummyTuples();
      return;
    }

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
          /*
           * Input data will have following entries.
           * 1st entry is stock symbol as String,
           * 2nd entry is stock price as Double,
           * 3rd entry is stock total daily volume as Long, and
           * 4th entry is stock last trade time String.
           *
           */
          String symbol = tuple.get(0);
          long currentVolume = Long.valueOf(tuple.get(2));
          long vol = currentVolume;
          // Sends total volume in first tick, and incremental volume afterwards.
          if (lastVolume.containsKey(symbol)) {
            vol -= lastVolume.get(symbol);
          }

          price.emit(new KeyValPair<String, Double>(symbol, new Double(tuple.get(1))));
          volume.emit(new KeyValPair<String, Long>(symbol, vol));
          if (logTime){
            time.emit(new KeyValPair<String, String>(symbol, tuple.get(3)));
          }
          lastVolume.put(symbol, currentVolume);
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

  /**
   *  Send Dummy tuple for testing.
   */
  public void emitDummyTuples()
  {
    int sym = random.nextInt(symbolList.size());

    // price
    double pr = random.nextInt(500) / 100.0;
    if (sym == 0) { // YHOO
      pr += 20;
    }
    else if (sym == 1) { // EBAY
      pr += 52;
    }
    else if (sym == 2) { // AAPL
      pr += 506;
    }
    else { // GOOG
      pr += 715;
    }

    // volume
    int vol = 10; // 0 + random.nextInt(100);

    if (price.isConnected()) {
      price.emit(new KeyValPair<String, Double>(symbolList.get(sym), new Double(pr)));
    }
    if (volume.isConnected()) {
      volume.emit(new KeyValPair<String, Long>(symbolList.get(sym), new Long(vol)));
    }
    if (time.isConnected()) {  // generate current time
      Date now = new Date();
      String strDate = sdf.format(now);
      time.emit(new KeyValPair<String, String>(symbolList.get(sym), strDate));
    }
  }
  /**
   *     The output port to emit price.
   */
  @OutputPortFieldAnnotation(name = "price", optional = true)
  public final transient DefaultOutputPort<KeyValPair<String, Double>> price = new DefaultOutputPort<KeyValPair<String, Double>>(this);
  /**
   *     The output port to emit incremental volume.
   */
  @OutputPortFieldAnnotation(name = "volume", optional = true)
  public final transient DefaultOutputPort<KeyValPair<String, Long>> volume = new DefaultOutputPort<KeyValPair<String, Long>>(this);
  /**
   *     The output port to emit last traded time.
   */
  @OutputPortFieldAnnotation(name = "time", optional = true)
  public final transient DefaultOutputPort<KeyValPair<String, String>> time = new DefaultOutputPort<KeyValPair<String, String>>(this);

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }
}
