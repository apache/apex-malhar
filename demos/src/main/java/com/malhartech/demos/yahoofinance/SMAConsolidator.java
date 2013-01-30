/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.algo.ConsolidatorKeyVal;
import com.malhartech.lib.util.KeyValPair;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consolidate simple moving average of price and volume.
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class SMAConsolidator extends ConsolidatorKeyVal<String, Double, Long>
{
  private static final Logger logger = LoggerFactory.getLogger(SMAConsolidator.class);
  /**
   * Output port which consolidate the key value pairs.
   */
  @OutputPortFieldAnnotation(name = "out")
  public final transient DefaultOutputPort<ConsolidatedTuple> out = new DefaultOutputPort<ConsolidatedTuple>(this);

  @Override
  public Object mergeKeyValue(String tuple_key, Object tuple_val, ArrayList<Object> list, int port)
  {
    if (port >= 0 && port < 2) { // smaPrice, smaVolume
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
                                                (Long)obj.getValue().get(1));

    //logger.debug(String.format("Emitted tuple: %s", t.toString()));
    out.emit(t);
  }

  /**
   * Consolidated tuple generated from all coming inputs.
   */
  public static class ConsolidatedTuple
  {
    private String symbol;
    private Double smaPrice;
    private Long smaVolume;

    private ConsolidatedTuple()
    {
    }

    public ConsolidatedTuple(String symbol, Double price, Long volume)
    {
      this.symbol = symbol;
      this.smaPrice = price;
      this.smaVolume = volume;
    }

    @Override
    public String toString()
    {
      return String.format("SMA   %4s price: %6.2f volume: %9d", symbol, smaPrice, smaVolume);
    }
  }
}
