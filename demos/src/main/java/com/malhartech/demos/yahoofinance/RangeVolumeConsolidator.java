/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.algo.ConsolidatorKeyVal;
import com.malhartech.lib.util.HighLow;
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
public class RangeVolumeConsolidator extends ConsolidatorKeyVal<String, HighLow, Long>
{
  private static final Logger logger = LoggerFactory.getLogger(RangeVolumeConsolidator.class);
  /**
   * Output port which consolidate the key value pairs.
   */
  @OutputPortFieldAnnotation(name = "out")
  public final transient DefaultOutputPort<ConsolidatedTuple> out = new DefaultOutputPort<ConsolidatedTuple>(this);

  @Override
  public Object mergeKeyValue(String tuple_key, Object tuple_val, ArrayList<Object> list, int port)
  {
    if (port >= 0 && port < 2) { // price(high & low), volume
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
    HighLow d = (HighLow)(o.get(0));

    ConsolidatedTuple t = new ConsolidatedTuple(obj.getKey(),
                                                d.getHigh().doubleValue(),
                                                d.getLow().doubleValue(),
                                                (Long)o.get(1));

    //logger.debug(String.format("Emitted tuple: %s", t.toString()));
    out.emit(t);
  }

  /**
   * Consolidated tuple generated from all coming inputs.
   */
  public static class ConsolidatedTuple // make static to help Kryo
  {
    private String symbol;
    private Double high;
    private Double low;
    private Long volume;

    private ConsolidatedTuple()
    {
    }

    public ConsolidatedTuple(String symbol, Double high, Double low, Long volume)
    {
      this.symbol = symbol;
      this.high = high;
      this.low = low;
      this.volume = volume;
    }

    @Override
    public String toString()
    {
      return String.format("Chart %4s high: %6.2f low: %6.2f volume: %9d", symbol, high, low, volume);
    }
  }
}
