/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.chart;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.lib.util.CandleStick;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * This is the chart operator that plots the candle stick of Y for each window.  X will be based on the timestamp derived from the window id
 * @param <K> The type of the key
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class TimeSeriesCandleStickChartOperator<K> extends TimeSeriesChartOperator<K, CandleStick>
{
  protected transient Map<K, CandleStick> dataMap = new TreeMap<K, CandleStick>();

  @Override
  public Type getChartType()
  {
    return Type.CANDLE;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    dataMap.clear();
  }

  @Override
  public CandleStick getY(K key)
  {
    return dataMap.get(key);
  }

  @Override
  public Collection<K> getKeys()
  {
    return dataMap.keySet();
  }

  @Override
  public void processTuple(Object tuple)
  {
    K key = convertTupleToKey(tuple);
    Number number = convertTupleToY(tuple);
    if (number != null) {
      if (yNumberType == NumberType.DOUBLE) {
        double value = number.doubleValue();
        CandleStick candleStick = dataMap.get(key);
        if (candleStick != null) {
          if (value > candleStick.getHigh().doubleValue()) {
            candleStick.setHigh(value);
          }
          if (value < candleStick.getLow().doubleValue()) {
            candleStick.setLow(value);
          }
          candleStick.setClose(value);
        }
        else {
          candleStick = new CandleStick(value, value, value, value);
          dataMap.put(key, candleStick);
        }
      }
      else {
        long value = number.longValue();
        CandleStick candleStick = dataMap.get(key);
        if (candleStick != null) {
          if (value > candleStick.getHigh().longValue()) {
            candleStick.setHigh(value);
          }
          if (value < candleStick.getLow().longValue()) {
            candleStick.setLow(value);
          }
          candleStick.setClose(value);
        }
        else {
          candleStick = new CandleStick(value, value, value, value);
          dataMap.put(key, candleStick);
        }
      }
    }

  }

}
