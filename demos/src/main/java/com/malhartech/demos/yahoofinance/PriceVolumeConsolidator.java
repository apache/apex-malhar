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
 * Consolidate price, volume and time. <p>
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class PriceVolumeConsolidator extends KeyValueConsolidator5<String, Double, Long, String, Integer, Double>
{
  private static final Logger logger = LoggerFactory.getLogger(PriceVolumeConsolidator.class);
  /**
   * Output port which consolidate the key value pairs.
   */
  @OutputPortFieldAnnotation(name = "out")
  public final transient DefaultOutputPort<ConsolidatedTuple> out = new DefaultOutputPort<ConsolidatedTuple>(this);

  @Override
  public Object mergeKeyValue(String tuple_key, Object tuple_val, ArrayList list, int port)
  {
    if (port >= 0 && port < 3) { // price, volume, time
      return tuple_val;
    }
    else {
      return null;
    }
  }

  @Override
  public void emitConsolidatedTuple(KeyValPair<String, ArrayList<Object>> obj)
  {
    ConsolidatedTuple t = new ConsolidatedTuple(obj.getKey(),
                                                (Double)obj.getValue().get(0),
                                                (Long)obj.getValue().get(1),
                                                (String)obj.getValue().get(2));

    //logger.debug(String.format("Emitted tuple: %s", t.toString()));
    out.emit(t);
  }

  /**
   * Consolidated tuple generated from all coming inputs.
   */
  public static class ConsolidatedTuple
  {
    private String symbol;
    private Double price;
    private Long totalVolume;
    private String time;

    public ConsolidatedTuple()
    {
    }

    public ConsolidatedTuple(String symbol, Double price, Long totalVolume, String time)
    {
      this.symbol = symbol;
      this.price = price;
      this.totalVolume = totalVolume;
      this.time = time;
    }

    @Override
    public String toString()
    {
      return String.format("Quote %4s price: %6.2f volume: %9d time: %s ", symbol, price, totalVolume, time);
    }
  }
}
