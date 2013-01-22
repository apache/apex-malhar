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
    if (port >= 0 && port < 4) { // price, volume, time, count
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
                                                (String)obj.getValue().get(2),
                                                (Integer)obj.getValue().get(3));

    //logger.debug(String.format("Emitted tuple: %s", t.toString()));
    out.emit(t);
  }

  /**
   * Consolidated tuple generated from all coming inputs.
   */
  public class ConsolidatedTuple
  {
    private String symbol;
    private Double price;
    private Long totalVolume;
    private String time;
    private Integer count;

    public ConsolidatedTuple(String symbol, Double price, Long totalVolume, String time, Integer count)
    {
      this.symbol = symbol;
      this.price = price;
      this.totalVolume = totalVolume;
      this.time = time;
      this.count = count;
    }

    @Override
    public String toString()
    {
      return String.format("%4s time: %s price: %6.2f volume: %9d count: %9d", symbol, time, price, totalVolume, count);
    }
  }
}
