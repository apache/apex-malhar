/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;

import com.malhartech.lib.stream.ConsolidatorKeyVal;
import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.math.RangeKeyVal;
import com.malhartech.lib.math.SumKeyVal;
import com.malhartech.lib.multiwindow.SimpleMovingAverage;
import org.apache.hadoop.conf.Configuration;

/**
 * Yahoo! Finance application demo. <p>
 *
 * Get Yahoo finance feed and calculate minute price range, minute volume, simple moving average of 5 minutes.
 */
public class Application implements ApplicationFactory
{
  private int streamingWindowSizeMilliSeconds = 1000; // 1 second (default is 500ms)
  private int appWindowCountMinute = 3 * 1000 / streamingWindowSizeMilliSeconds;   // 1 minute
  private int appWindowCountSMA = 5 * 1000 / streamingWindowSizeMilliSeconds;  // 5 minute
  private String[] tickers = {"IBM", "GOOG", "AAPL", "YHOO"};

  /**
   * Get actual Yahoo finance ticks of symbol, last price, total daily volume, and last traded price.
   */
  public StockTickInput getStockTickInputOperator(String name, DAG dag)
  {
    StockTickInput oper = dag.addOperator(name, StockTickInput.class);
    oper.readIntervalMillis = 200;
    oper.symbols = tickers;
    return oper;
  }

  /**
   * This sends total daily volume by adding volumes from each ticks.
   */
  public SumKeyVal<String, Long> getDailyVolumeOperator(String name, DAG dag)
  {
    SumKeyVal<String, Long> oper = dag.addOperator(name, new SumKeyVal<String, Long>());
    oper.setType(Long.class);
    oper.setCumulative(true);
    oper.setEmitOnlyWhenChanged(true);
    return oper;
  }

  /**
   * Get aggregated volume of 1 minute and send at the end window of 1 minute.
   */
  public SumKeyVal<String, Long> getMinuteVolumeOperator(String name, DAG dag, int appWindowCount)
  {
    SumKeyVal<String, Long> oper = dag.addOperator(name, new SumKeyVal<String, Long>());
    oper.setType(Long.class);
    oper.setEmitOnlyWhenChanged(true);
    dag.getOperatorWrapper(name).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(appWindowCount);
    return oper;
  }

  /**
   * Get High-low range for 1 minute.
   */
  public RangeKeyVal<String, Double> getHighLowOperator(String name, DAG dag, int appWindowCount)
  {
    RangeKeyVal<String, Double> oper = dag.addOperator(name, new RangeKeyVal<String, Double>());
    dag.getOperatorWrapper(name).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(appWindowCount);
    oper.setType(Double.class);
    return oper;
  }

  /**
   * Quote (Merge price, daily volume, time)
   */
  public ConsolidatorKeyVal<String> getQuoteOperator(String name, DAG dag)
  {
    ConsolidatorKeyVal<String> oper = dag.addOperator(name, new ConsolidatorKeyVal<String>());
    return oper;
  }

  /**
   * Chart (Merge minute volume and minute high-low)
   */
  public ConsolidatorKeyVal<String> getChartOperator(String name, DAG dag)
  {
    ConsolidatorKeyVal<String> oper = dag.addOperator(name, new ConsolidatorKeyVal<String>());
    return oper;
  }

  /**
   * Get simple moving average of price.
   */
  public SimpleMovingAverage<String, Double> getPriceSimpleMovingAverageOperator(String name, DAG dag, int appWindowCount)
  {
    SimpleMovingAverage<String, Double> oper = dag.addOperator(name, new SimpleMovingAverage<String, Double>());
    oper.setWindowSize(appWindowCount);
    oper.setType(Double.class);
    return oper;
  }

  /**
   * Get console for output.
   */
  public InputPort<Object> getConsole(String name, /*String nodeName,*/ DAG dag, String prefix)
  {
    // hack to output to HTTP based on actual environment
        /*
     String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
     if (serverAddr != null) {
     HttpOutputOperator<Object> oper = dag.addOperator(name, new HttpOutputOperator<Object>());
     oper.setResourceURL(URI.create("http://" + serverAddr + "/channel/" + nodeName));
     return oper.input;
     }
     */

    ConsoleOutputOperator oper = dag.addOperator(name, ConsoleOutputOperator.class);
    oper.setStringFormat(prefix + ": %s");
    return oper.input;
  }

  /**
   * Create Yahoo Finance Application DAG.
   */
  @Override
  @SuppressWarnings("unchecked")
  public DAG getApplication(Configuration conf)
  {
    DAG dag = new DAG(conf);

    dag.getAttributes().attr(DAG.STRAM_WINDOW_SIZE_MILLIS).set(streamingWindowSizeMilliSeconds);

    StockTickInput tick = getStockTickInputOperator("StockTickInput", dag);
    SumKeyVal<String, Long> dailyVolume = getDailyVolumeOperator("DailyVolume", dag);
    ConsolidatorKeyVal<String> quoteOperator = getQuoteOperator("Quote", dag);

    RangeKeyVal<String, Double> highlow = getHighLowOperator("HighLow", dag, appWindowCountMinute);
    SumKeyVal<String, Long> minuteVolume = getMinuteVolumeOperator("MinuteVolume", dag, appWindowCountMinute);
    ConsolidatorKeyVal<String> chartOperator = getChartOperator("Chart", dag);

    SimpleMovingAverage<String, Double> priceSMA = getPriceSimpleMovingAverageOperator("PriceSMA", dag, appWindowCountSMA);

    dag.addStream("price", tick.price).addSink(quoteOperator.in1).addSink(highlow.data).addSink(priceSMA.data);
    dag.addStream("vol", tick.volume, dailyVolume.data, minuteVolume.data);
    dag.addStream("time", tick.time).addSink(quoteOperator.in3);
    dag.addStream("daily_vol", dailyVolume.sum).addSink(quoteOperator.in2);

    dag.addStream("quote_data", quoteOperator.out, getConsole("quoteConsole", dag, "QUOTE"));

    dag.addStream("high_low", highlow.range).addSink(chartOperator.in1);
    dag.addStream("vol_1min", minuteVolume.sum).addSink(chartOperator.in2);
    dag.addStream("chart_data", chartOperator.out, getConsole("chartConsole", dag, "CHART"));

    dag.addStream("sma_price", priceSMA.doubleSMA, getConsole("priceSMAConsole", dag, "Price SMA"));

    return dag;
  }

}
