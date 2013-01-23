/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import com.malhartech.lib.util.KeyValPair;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This generates dummy data used for testing.
 *
 * This generates
 * Stock symbol as String,
 * Stock price as Double,
 * Stock volume as Long, and
 * Stock time String.
 * 
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class DummyStockTickInput implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(DummyStockTickInput.class);

  private final Random random = new Random();
  private ArrayList<String> symbols = new ArrayList<String>();
  private final transient SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS"); // new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  /**
   * The output port that will emit tuple into DAG.
   */
  @OutputPortFieldAnnotation(name = "price", optional = true)
  public final transient DefaultOutputPort<KeyValPair<String, Double>> price = new DefaultOutputPort<KeyValPair<String, Double>>(this);
  @OutputPortFieldAnnotation(name = "volume", optional = true)
  public final transient DefaultOutputPort<KeyValPair<String, Long>> volume = new DefaultOutputPort<KeyValPair<String, Long>>(this);
  @OutputPortFieldAnnotation(name = "time", optional = true)
  public final transient DefaultOutputPort<KeyValPair<String, String>> time = new DefaultOutputPort<KeyValPair<String, String>>(this);

  @Override
  public void emitTuples()
  {
    int sym = random.nextInt(4);

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
      price.emit(new KeyValPair(symbols.get(sym), new Double(pr)));
    }
    if (volume.isConnected()) {
      volume.emit(new KeyValPair(symbols.get(sym), new Long(vol)));
    }
    if (time.isConnected()) {  // generate current time
      Date now = new Date();
      String strDate = sdf.format(now);
      time.emit(new KeyValPair(symbols.get(sym), strDate));
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

  @Override
  public void setup(OperatorContext context)
  {
    symbols.add("YHOO");
    symbols.add("EBAY");
    symbols.add("AAPL");
    symbols.add("GOOG");
  }

  @Override
  public void teardown()
  {
  }
}
