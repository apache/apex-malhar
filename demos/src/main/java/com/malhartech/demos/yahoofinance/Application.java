/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.math.RangeKeyVal;
import com.malhartech.lib.math.SumKeyVal;
import com.malhartech.lib.multiwindow.SimpleMovingAverage;
import com.malhartech.lib.stream.ConsolidatorKeyVal;
import com.malhartech.lib.util.HighLow;
import org.apache.hadoop.conf.Configuration;

/**
 * Yahoo! Finance application demo. <p>
 *
 * Get Yahoo finance feed and calculate minute price range, minute volume, simple moving average of 5 minutes.
 */
public class Application implements ApplicationFactory
{
  protected int streamingWindowSizeMilliSeconds = 1000; // 1 second (default is 500ms)
  protected int appWindowCountMinute = 3 * 1000 / streamingWindowSizeMilliSeconds;   // 1 minute
  protected int appWindowCountSMA = 5 * 1000 / streamingWindowSizeMilliSeconds;  // 5 minute
  protected String[] tickers = {"IBM", "GOOG", "AAPL", "YHOO"};

  /**
   * Get actual Yahoo finance ticks of symbol, last price, total daily volume, and last traded price.
   * @param name
   * @param dag
   * @return
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
   * @param name
   * @param dag
   * @return
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
   * @param name
   * @param dag
   * @param appWindowCount
   * @return
   */
  public SumKeyVal<String, Long> getMinuteVolumeOperator(String name, DAG dag, int appWindowCount)
  {
    SumKeyVal<String, Long> oper = dag.addOperator(name, new SumKeyVal<String, Long>());
    oper.setType(Long.class);
    oper.setEmitOnlyWhenChanged(true);
    dag.getOperatorMeta(name).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(appWindowCount);
    return oper;
  }

  /**
   * Get High-low range for 1 minute.
   * @param name
   * @param dag
   * @param appWindowCount
   * @return
   */
  public RangeKeyVal<String, Double> getHighLowOperator(String name, DAG dag, int appWindowCount)
  {
    RangeKeyVal<String, Double> oper = dag.addOperator(name, new RangeKeyVal<String, Double>());
    dag.getOperatorMeta(name).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(appWindowCount);
    oper.setType(Double.class);
    return oper;
  }

  /**
   * Quote (Merge price, daily volume, time)
   * @param name
   * @param dag
   * @return
   */
  public ConsolidatorKeyVal<String,Double,Long,String,?,?> getQuoteOperator(String name, DAG dag)
  {
    ConsolidatorKeyVal<String,Double,Long,String,?,?> oper = dag.addOperator(name, new ConsolidatorKeyVal<String,Double,Long,String,Object,Object>());
    return oper;
  }

  /**
   * Chart (Merge minute volume and minute high-low)
   * @param name
   * @param dag
   * @return
   */
  public ConsolidatorKeyVal<String,HighLow,Long,?,?,?> getChartOperator(String name, DAG dag)
  {
    ConsolidatorKeyVal<String,HighLow,Long,?,?,?> oper = dag.addOperator(name, new ConsolidatorKeyVal<String,HighLow,Long,Object,Object,Object>());
    return oper;
  }

  /**
   * Get simple moving average of price.
   * @param name
   * @param dag
   * @param appWindowCount
   * @return
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
   * @param name
   * @param dag
   * @param prefix
   * @return
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
   * @param conf
   * @return
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    dag.getAttributes().attr(DAG.STRAM_WINDOW_SIZE_MILLIS).set(streamingWindowSizeMilliSeconds);

    StockTickInput tick = getStockTickInputOperator("StockTickInput", dag);
    SumKeyVal<String, Long> dailyVolume = getDailyVolumeOperator("DailyVolume", dag);
    ConsolidatorKeyVal<String,Double,Long,String,?,?> quoteOperator = getQuoteOperator("Quote", dag);

    RangeKeyVal<String, Double> highlow = getHighLowOperator("HighLow", dag, appWindowCountMinute);
    SumKeyVal<String, Long> minuteVolume = getMinuteVolumeOperator("MinuteVolume", dag, appWindowCountMinute);
    ConsolidatorKeyVal<String,HighLow,Long,?,?,?> chartOperator = getChartOperator("Chart", dag);

    SimpleMovingAverage<String, Double> priceSMA = getPriceSimpleMovingAverageOperator("PriceSMA", dag, appWindowCountSMA);

    dag.addStream("price", tick.price, quoteOperator.in1, highlow.data, priceSMA.data);
    dag.addStream("vol", tick.volume, dailyVolume.data, minuteVolume.data);
    dag.addStream("time", tick.time, quoteOperator.in3);
    dag.addStream("daily_vol", dailyVolume.sum, quoteOperator.in2);

    dag.addStream("quote_data", quoteOperator.out, getConsole("quoteConsole", dag, "QUOTE"));

    dag.addStream("high_low", highlow.range, chartOperator.in1);
    dag.addStream("vol_1min", minuteVolume.sum, chartOperator.in2);
    dag.addStream("chart_data", chartOperator.out, getConsole("chartConsole", dag, "CHART"));

    dag.addStream("sma_price", priceSMA.doubleSMA, getConsole("priceSMAConsole", dag, "Price SMA"));

  }

}
