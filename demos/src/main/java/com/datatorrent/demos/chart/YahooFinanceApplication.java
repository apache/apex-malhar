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
package com.datatorrent.demos.chart;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.demos.yahoofinance.StockTickInput;
import com.datatorrent.lib.chart.CandleStick;
import com.datatorrent.lib.chart.TimeSeriesAverageChartOperator;
import com.datatorrent.lib.chart.TimeSeriesCandleStickChartOperator;
import com.datatorrent.lib.chart.XYChartOperator.NumberType;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.lib.util.KeyValPair;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * <p>YahooFinanceApplication class.</p>
 *
 * @since 0.3.2
 */
public class YahooFinanceApplication extends com.datatorrent.demos.yahoofinance.Application
{
  public static class YahooFinanceTimeSeriesAverageChartOperator extends TimeSeriesAverageChartOperator<String>
  {

    @Override
    public Number convertTupleToY(Object tuple)
    {
      KeyValPair<String, Double> kvp = (KeyValPair<String, Double>)tuple;
      return kvp.getValue();
    }

    @Override
    public String convertTupleToKey(Object tuple)
    {
      KeyValPair<String, Double> kvp = (KeyValPair<String, Double>)tuple;
      return kvp.getKey();
    }

  }

  public static class YahooFinanceTimeSeriesCandleStickChartOperator extends TimeSeriesCandleStickChartOperator<String>
  {
    @Override
    public Number convertTupleToY(Object tuple)
    {
      KeyValPair<String, Double> kvp = (KeyValPair<String, Double>)tuple;
      return kvp.getValue();
    }

    @Override
    public String convertTupleToKey(Object tuple)
    {
      KeyValPair<String, Double> kvp = (KeyValPair<String, Double>)tuple;
      return kvp.getKey();
    }

  }

  TimeSeriesAverageChartOperator<String> getAverageChartOperator(String name, DAG dag)
  {
    YahooFinanceTimeSeriesAverageChartOperator op = new YahooFinanceTimeSeriesAverageChartOperator();
    op.setxAxisLabel("TIME");
    op.setyAxisLabel("PRICE");
    op.setyNumberType(NumberType.FLOAT);
    return dag.addOperator(name, op);
  }

  TimeSeriesCandleStickChartOperator<String> getCandleStickChartOperator(String name, DAG dag)
  {
    YahooFinanceTimeSeriesCandleStickChartOperator op = new YahooFinanceTimeSeriesCandleStickChartOperator();
    op.setxAxisLabel("TIME");
    op.setyAxisLabel("PRICE");
    op.setyNumberType(NumberType.FLOAT);
    return dag.addOperator(name, op);
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.getAttributes().attr(DAG.STREAMING_WINDOW_SIZE_MILLIS).set(streamingWindowSizeMilliSeconds);

    StockTickInput tick = getStockTickInputOperator("StockTickInput", dag);
    tick.setOutputEvenIfZeroVolume(true);
    DAG.StreamMeta stream = dag.addStream("price", tick.price);
    TimeSeriesAverageChartOperator<String> averageChartOperator = getAverageChartOperator("AverageChart", dag);
    TimeSeriesCandleStickChartOperator<String> candleStickChartOperator = getCandleStickChartOperator("CandleStickChart", dag);
    DevNull<Map<String, Map<Number, Number>>> devnull1 = dag.addOperator("devnull1", new DevNull<Map<String, Map<Number, Number>>>());
    DevNull<Map<String, Map<Number, CandleStick>>> devnull2 = dag.addOperator("devnull2", new DevNull<Map<String, Map<Number, CandleStick>>>());
    dag.getMeta(averageChartOperator).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(5); // 5 seconds
    dag.getMeta(candleStickChartOperator).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(5); // 5 seconds
    stream.addSink(averageChartOperator.in1);
    stream.addSink(candleStickChartOperator.in1);
    dag.addStream("averageDummyStream", averageChartOperator.chart, devnull1.data);
    dag.addStream("candleStickDummyStream", candleStickChartOperator.chart, devnull2.data);

  }

}
