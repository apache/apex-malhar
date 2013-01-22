/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.algo.KeyValueConsolidator5;
import com.malhartech.lib.util.KeyValPair;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consolidate Price high low and volume.
 * 1st parameter is stock symbol as key String.
 * 2nd parameter is high-low price range as ArrayList<Double>
 * 3rd parameter is total volume as Long.
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class RangeVolumeConsolidator extends KeyValueConsolidator5<String, ArrayList<Double>, Long, Long, Integer, Double>
{
  private static final Logger logger = LoggerFactory.getLogger(RangeVolumeConsolidator.class);
  /**
   * Output port which consolidate the key value pairs.
   */
  @OutputPortFieldAnnotation(name = "out")
  public final transient DefaultOutputPort<ConsolidatedTuple> out = new DefaultOutputPort<ConsolidatedTuple>(this);

  @Override
  public Object mergeKeyValue(String tuple_key, Object tuple_val, ArrayList list, int port)
  {
    if (port >= 0 && port < 4) { // price(high & low), volume, time, count
      return tuple_val;
    }
    else {
      return null;
    }
  }

  @Override
  public void emitConsolidatedTuple(KeyValPair<String, ArrayList<Object>> obj)
  {
    ArrayList<Object> o = obj.getValue();
    ArrayList<Double> d = (ArrayList<Double>)(o.get(0));

    ConsolidatedTuple t = new ConsolidatedTuple(obj.getKey(),
                                                d.get(0),
                                                d.get(1),
                                                (Long)o.get(1),
                                                (Long)o.get(2),
                                                (Integer)o.get(3));

    //logger.debug(String.format("Emitted tuple: %s", t.toString()));
    out.emit(t);
  }

  /**
   * Consolidated tuple generated from all coming inputs.
   */
  public class ConsolidatedTuple
  {
    private String symbol;
    private Double high;
    private Double low;
    private Long volume;
    private Long average;
    private Integer count;

    public ConsolidatedTuple(String symbol, Double high, Double low, Long volume, Long average, Integer count)
    {
      this.symbol = symbol;
      this.high = high;
      this.low = low;
      this.volume = volume;
      this.average = average;
      this.count = count;
    }

    @Override
    public String toString()
    {
      return String.format("%4s high: %6.2f low: %6.2f volume: %9d average: %9d count: %9d", symbol, high, low, volume, average, count);
    }
  }
}
