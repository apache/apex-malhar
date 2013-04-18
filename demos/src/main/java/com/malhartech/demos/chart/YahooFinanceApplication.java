/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.chart;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.api.DAG.StreamMeta;
import com.malhartech.demos.yahoofinance.StockTickInput;
import com.malhartech.lib.chart.TimeSeriesAverageChartOperator;
import com.malhartech.lib.chart.TimeSeriesCandleStickChartOperator;
import com.malhartech.lib.stream.DevNull;
import com.malhartech.lib.util.CandleStick;
import com.malhartech.lib.util.KeyValPair;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class YahooFinanceApplication extends com.malhartech.demos.yahoofinance.Application
{
  public static class YahooFinanceTimeSeriesAverageChartOperator extends TimeSeriesAverageChartOperator
  {

    @Override
    public Number convertTupleToNumber(Object tuple)
    {
      KeyValPair<String, Double> kvp = (KeyValPair<String, Double>)tuple;
      return kvp.getValue();
    }

    @Override
    public Object convertTupleToKey(Object tuple)
    {
      KeyValPair<String, Double> kvp = (KeyValPair<String, Double>)tuple;
      return kvp.getKey();
    }

  }

  public static class YahooFinanceTimeSeriesCandleStickChartOperator extends TimeSeriesCandleStickChartOperator
  {
    public String ticker;

    @Override
    public Number convertTupleToNumber(Object tuple)
    {
      KeyValPair<String, Double> kvp = (KeyValPair<String, Double>)tuple;
      return kvp.getValue();
    }

    @Override
    public Object convertTupleToKey(Object tuple)
    {
      KeyValPair<String, Double> kvp = (KeyValPair<String, Double>)tuple;
      return kvp.getKey();
    }

  }

  TimeSeriesAverageChartOperator getAverageChartOperator(String name, DAG dag)
  {
    YahooFinanceTimeSeriesAverageChartOperator op = new YahooFinanceTimeSeriesAverageChartOperator();
    return dag.addOperator(name, op);
  }

  TimeSeriesCandleStickChartOperator getCandleStickChartOperator(String name, DAG dag)
  {
    YahooFinanceTimeSeriesCandleStickChartOperator op = new YahooFinanceTimeSeriesCandleStickChartOperator();
    return dag.addOperator(name, op);
  }

  @Override
  public DAG getApplication(Configuration conf)
  {
    DAG dag = new DAG(conf);

    dag.getAttributes().attr(DAG.STRAM_WINDOW_SIZE_MILLIS).set(streamingWindowSizeMilliSeconds);

    StockTickInput tick = getStockTickInputOperator("StockTickInput", dag);
    tick.setOutputEvenIfZeroVolume(true);
    StreamMeta stream = dag.addStream("price", tick.price);
    TimeSeriesAverageChartOperator averageChartOperator = getAverageChartOperator("AverageChart", dag);
    TimeSeriesCandleStickChartOperator candleStickChartOperator = getCandleStickChartOperator("CandleStickChart", dag);
    DevNull<Map<Object, KeyValPair<Number, Number>>> devnull1 = dag.addOperator("devnull1", new DevNull<Map<Object, KeyValPair<Number, Number>>>());
    DevNull<Map<Object, KeyValPair<Number, CandleStick>>> devnull2 = dag.addOperator("devnull2", new DevNull<Map<Object, KeyValPair<Number, CandleStick>>>());
    dag.getOperatorMeta(averageChartOperator).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(5); // 5 seconds
    dag.getOperatorMeta(candleStickChartOperator).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(5); // 5 seconds
    stream.addSink(averageChartOperator.in1);
    stream.addSink(candleStickChartOperator.in1);
    dag.addStream("averageDummyStream", averageChartOperator.chart, devnull1.data);
    dag.addStream("candleStickDummyStream", candleStickChartOperator.chart, devnull2.data);

    return dag;
  }

}
