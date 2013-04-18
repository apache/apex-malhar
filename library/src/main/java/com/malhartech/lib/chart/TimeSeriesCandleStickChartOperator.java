/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.chart;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.lib.util.CandleStick;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class TimeSeriesCandleStickChartOperator extends TimeSeriesChartOperator<CandleStick>
{
  Map<Object, CandleStick> dataMap = new HashMap<Object, CandleStick>();

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
  public CandleStick getY(Object key)
  {
    return dataMap.get(key);
  }

  @Override
  public Collection<Object> getKeys()
  {
    return dataMap.keySet();
  }

  @Override
  public void processTuple(Object tuple)
  {
    Object key = convertTupleToKey(tuple);
    Number number = convertTupleToNumber(tuple);
    if (number != null) {
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
      } else {
        candleStick = new CandleStick(value, value, value, value);
        dataMap.put(key, candleStick);
      }
    }

  }

}
