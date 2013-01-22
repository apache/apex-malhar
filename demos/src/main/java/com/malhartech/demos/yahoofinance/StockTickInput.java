/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.KeyValPair;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class StockTickInput extends BaseOperator
{
  HashMap<String, Long> startVolume = new HashMap<String, Long>();
  HashMap<String, Long> lastVolume = new HashMap<String, Long>();

  // Input data will have following entries.
  // 1st entry is stock symbol as String,
  // 2nd entry is stock price as Double,
  // 3rd entry is stock total daily volume as Long, and
  // 4th entry is stock last trade time String.
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<ArrayList<String>> data = new DefaultInputPort<ArrayList<String>>(this)
  {
    @Override
    public void process(ArrayList<String> tuple)
    {
      if (tuple.size() != 4) {
        return;
      }
      String symbol = tuple.get(0);

      if (!lastVolume.containsKey(symbol)){
        long vol = Long.valueOf(tuple.get(2));
        startVolume.put(symbol, vol);
        lastVolume.put(symbol, vol);
        return;
      }

      long currentVolume = Long.valueOf(tuple.get(2));
      price.emit(new KeyValPair(symbol, new Double(tuple.get(1))));
      volume.emit(new KeyValPair(symbol, currentVolume-lastVolume.get(symbol)));
      time.emit(new KeyValPair(symbol, tuple.get(3)));
      lastVolume.put(symbol,currentVolume);
    }
  };
  /**
   * The output port that will emit tuple into DAG.
   */
  @OutputPortFieldAnnotation(name = "price", optional = true)
  public final transient DefaultOutputPort<KeyValPair<String, Double>> price = new DefaultOutputPort<KeyValPair<String, Double>>(this);
  @OutputPortFieldAnnotation(name = "volume", optional = true)
  public final transient DefaultOutputPort<KeyValPair<String, Long>> volume = new DefaultOutputPort<KeyValPair<String, Long>>(this);
  @OutputPortFieldAnnotation(name = "time", optional = true)
  public final transient DefaultOutputPort<KeyValPair<String, String>> time = new DefaultOutputPort<KeyValPair<String, String>>(this);
}
