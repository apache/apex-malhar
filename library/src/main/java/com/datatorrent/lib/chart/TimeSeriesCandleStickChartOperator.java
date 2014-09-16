/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.chart;

import com.datatorrent.api.Context.OperatorContext;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * This is a chart operator that produces a candlestick plot out of values.
 *
 * This is the chart operator that plots the candle stick of Y for each window.  X will be based on the timestamp derived from the window id
 *
 * @displayName Time Series Candle Stick Chart Operator
 * @category chart
 * @tags output
 *
 * @param <K> The type of the key
 * @since 0.3.2
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
  public Collection<K> retrieveKeys()
  {
    return dataMap.keySet();
  }

  @Override
  public void processTuple(Object tuple)
  {
    K key = convertTupleToKey(tuple);
    Number number = convertTupleToY(tuple);
    if (number != null) {
      if (yNumberType == NumberType.FLOAT) {
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
