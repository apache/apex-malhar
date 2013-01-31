/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.math.AverageKeyVal;
import com.malhartech.lib.math.RangeKeyVal;
import com.malhartech.lib.math.SumKeyVal;
import com.malhartech.lib.multiwindow.SimpleMovingAverage;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Yahoo finance application demo. <p>
 *
 * Get Yahoo finance feed and calculate minute price range, minute volume, simple moving average of 5 minutes.
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class YahooFinanceApplication implements ApplicationFactory
{
  private static final Logger logger = LoggerFactory.getLogger(YahooFinanceApplication.class);

  /**
   * Get actual Yahoo finance ticks of symbol, last price, total daily volume, and last traded price.
   *
   * @param name
   * @param dag
   * @return inputs
   */
  public StockTickInput getTicks(String name, DAG dag, boolean dummy, boolean logTime)
  {
    StockTickInput oper = dag.addOperator(name, StockTickInput.class);
    oper.setReadIntervalMillis(200);
    oper.setLogTime(logTime);
    oper.addSymbol("YHOO");
    //oper.addSymbol("EBAY");
    oper.addSymbol("AAPL");
    //oper.addSymbol("GOOG");
    oper.setIsDummy(dummy);
    return oper;
  }

  /**
   * This sends total daily volume by adding volumes from each ticks.
   *
   * @param name
   * @param dag
   * @return daily volume
   */
  public SumKeyVal<String, Long> getDailyVolume(String name, DAG dag)
  {
    SumKeyVal<String, Long> oper = dag.addOperator(name, new SumKeyVal<String, Long>());
    oper.setType(Long.class);
    oper.setCumulative(true);
    return oper;
  }

  /**
   * Get cumulative volume of 1 minute and send at the end window of 1 minute.
   *
   * @param name
   * @param dag
   * @param appWindowCount
   * @return minute volume
   */
  public SumKeyVal<String, Long> getTimedVolume(String name, DAG dag, int appWindowCount)
  {
    SumKeyVal<String, Long> oper = dag.addOperator(name, new SumKeyVal<String, Long>());
    oper.setType(Long.class);
    dag.getOperatorWrapper(name).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(appWindowCount);
    return oper;
  }

  /**
   * Get High-low range for 1 minute.
   *
   * @param name
   * @param dag
   * @param appWindowCount
   * @return high-low range of 1 minute
   */
  public RangeKeyVal<String, Double> getTimedPriceRange(String name, DAG dag, int appWindowCount)
  {
    RangeKeyVal<String, Double> oper = dag.addOperator(name, new RangeKeyVal<String, Double>());
    dag.getOperatorWrapper(name).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(appWindowCount);
    oper.setType(Double.class);
    return oper;
  }

  /**
   * Merge price, daily volume, time.
   *
   * @param name
   * @param dag
   * @return merge of price, daily volume, and time
   */
  public PriceVolumeConsolidator getPriceVolumeConsolidator(String name, DAG dag)
  {
    PriceVolumeConsolidator oper = dag.addOperator(name, PriceVolumeConsolidator.class);
    return oper;
  }

  /**
   * Merge minute volume and minute high-low.
   *
   * @param name
   * @param dag
   * @return merge of minute volume and price range
   */
  public RangeVolumeConsolidator getRangeVolumeConsolidator(String name, DAG dag)
  {
    RangeVolumeConsolidator oper = dag.addOperator(name, RangeVolumeConsolidator.class);
    return oper;
  }

  /**
   * Merge SMA of price and volume.
   *
   * @param name
   * @param dag
   * @return merge sma of price and volume
   */
  public SMAConsolidator getSMAConsolidator(String name, DAG dag)
  {
    SMAConsolidator oper = dag.addOperator(name, SMAConsolidator.class);
    return oper;
  }

  /**
   * Get average price of streaming window.
   *
   * @param name
   * @param dag
   * @return average price of streaming window
   */
  public AverageKeyVal<String> getPriceAverage(String name, DAG dag)
  {
    AverageKeyVal<String> oper = dag.addOperator(name, new AverageKeyVal<String>());
    //oper.setType(Double.class);
    return oper;
  }

  /**
   * Get average volume of streaming window.
   *
   * @param name
   * @param dag
   * @return average volume of streaming window
   */
  public AverageKeyVal<String> getVolumeAverage(String name, DAG dag)
  {
    AverageKeyVal<String> oper = dag.addOperator(name, new AverageKeyVal<String>());
    return oper;
  }

  /**
   * Get simple moving average of price.
   *
   * @param name
   * @param dag
   * @param appWindowCount
   * @return simple moving average of price
   */
  public SimpleMovingAverage<String, Double> getPriceSimpleMovingAverage(String name, DAG dag, int appWindowCount)
  {
    SimpleMovingAverage<String, Double> oper = dag.addOperator(name, new SimpleMovingAverage<String, Double>());
    oper.setWindowSize(appWindowCount);
    oper.setType(Double.class);
    return oper;
  }

  /**
   * Get simple moving average of volume.
   *
   * @param name
   * @param dag
   * @param appWindowCount
   * @return simple moving average of price
   */
  public SimpleMovingAverage<String, Long> getVolumeSimpleMovingAverage(String name, DAG dag, int appWindowCount)
  {
    SimpleMovingAverage<String, Long> oper = dag.addOperator(name, new SimpleMovingAverage<String, Long>());
    oper.setWindowSize(appWindowCount);
    oper.setType(Long.class);
    return oper;
  }

  /**
   * Get console for simple moving average.
   *
   * @param name
   * @param dag
   * @return priceSMA console
   */
  public ConsoleOutputOperator getConsole(String name, DAG dag)
  {
    ConsoleOutputOperator oper = dag.addOperator(name, ConsoleOutputOperator.class);
    return oper;
  }

  /**
   * Create DAG
   *
   * @param conf
   * @return dag
   */
  @Override
  public DAG getApplication(Configuration conf)
  {
    DAG dag = new DAG(conf);
    int streamingWindowSizeMilliSeconds = 1000; // Default streaming window size is 500 msec. Set this to 1 sec.
    dag.getAttributes().attr(DAG.STRAM_WINDOW_SIZE_MILLIS).set(streamingWindowSizeMilliSeconds);
    boolean allInline = false;
    boolean shouldbeInline = true;
    boolean isDummy = false;  // true for dummy data
    int appWindowCountMinute = 60 * 1000 / streamingWindowSizeMilliSeconds;   // 1 minute
    int appWindowCountSMA = 5 * 60 * 1000 / streamingWindowSizeMilliSeconds;  // 5 minute
    String test = "all"; // can be window, time, sma, all

    if (test.equals("window")) {
      StockTickInput tick = getTicks("tick", dag, isDummy, true);
      SumKeyVal<String, Long> dailyVolume = getDailyVolume("dailyVolume", dag);
      PriceVolumeConsolidator pvConsolidator = getPriceVolumeConsolidator("pvConsolidator", dag);
      ConsoleOutputOperator windowedConsole = getConsole("windowedConsole", dag);

      dag.addStream("volume_tick", tick.volume, dailyVolume.data).setInline(allInline);
      dag.addStream("price_tick", tick.price, pvConsolidator.data1).setInline(allInline);
      dag.addStream("time_tick", tick.time, pvConsolidator.data3).setInline(allInline);
      dag.addStream("volume_pvConsolidator", dailyVolume.sum, pvConsolidator.data2).setInline(shouldbeInline);
      dag.addStream("pvConsolidator_console", pvConsolidator.out, windowedConsole.input).setInline(shouldbeInline);
    }
    else if (test.equals("time")) {
      StockTickInput tick = getTicks("tick", dag, isDummy, false);
      RangeKeyVal<String, Double> highlow = getTimedPriceRange("highlow", dag, appWindowCountMinute);
      SumKeyVal<String, Long> minuteVolume = getTimedVolume("timedVolume", dag, appWindowCountMinute);
      RangeVolumeConsolidator rvConsolidator = getRangeVolumeConsolidator("rvConsolidator", dag);
      ConsoleOutputOperator minuteConsole = getConsole("timedConsole", dag);

      dag.addStream("price_tick", tick.price, highlow.data).setInline(allInline);
      dag.addStream("volume_tick", tick.volume, minuteVolume.data).setInline(allInline);
      dag.addStream("highlow_merge", highlow.range, rvConsolidator.data1).setInline(shouldbeInline);
      dag.addStream("volume_merge", minuteVolume.sum, rvConsolidator.data2).setInline(shouldbeInline);
      dag.addStream("minute_console", rvConsolidator.out, minuteConsole.input).setInline(shouldbeInline);
    }
    else if (test.equals("sma")) {
      StockTickInput tick = getTicks("tick", dag, isDummy, false);
      AverageKeyVal<String> priceAvg = getPriceAverage("priceAvg", dag);
      AverageKeyVal<String> volumeAvg = getVolumeAverage("volumeAvg", dag);
      SimpleMovingAverage<String, Double> priceSMA = getPriceSimpleMovingAverage("smaPrice", dag, appWindowCountSMA);
      SimpleMovingAverage<String, Long> volumeSMA = getVolumeSimpleMovingAverage("smaVolume", dag, appWindowCountSMA);
      SMAConsolidator smaConsolidator = getSMAConsolidator("smaConsolidator", dag);
      ConsoleOutputOperator smaConsole = getConsole("smaConsole", dag);

      dag.addStream("price_tick", tick.price, priceAvg.data).setInline(allInline);
      dag.addStream("priceAverage_priceSma", priceAvg.doubleAverage, priceSMA.data).setInline(shouldbeInline);
      dag.addStream("priceSma_smaCconsolidator", priceSMA.doubleSMA, smaConsolidator.data1).setInline(shouldbeInline);
      dag.addStream("volume_tick", tick.volume, volumeAvg.data).setInline(allInline);
      dag.addStream("volumeAverage_volumeSma", volumeAvg.longAverage, volumeSMA.data).setInline(shouldbeInline);
      dag.addStream("volumeSma_smaCconsolidator", volumeSMA.longSMA, smaConsolidator.data2).setInline(shouldbeInline);
      dag.addStream("smaConsolidator_console", smaConsolidator.out, smaConsole.input).setInline(shouldbeInline);
    }
    else if (test.equals("all")) {
      StockTickInput tick = getTicks("tick", dag, isDummy, true);
      SumKeyVal<String, Long> dailyVolume = getDailyVolume("dailyVolume", dag);
      PriceVolumeConsolidator pvConsolidator = getPriceVolumeConsolidator("pvConsolidator", dag);
      ConsoleOutputOperator windowedConsole = getConsole("windowedConsole", dag);
      RangeKeyVal<String, Double> highlow = getTimedPriceRange("highlow", dag, appWindowCountMinute);
      SumKeyVal<String, Long> minuteVolume = getTimedVolume("timedVolume", dag, appWindowCountMinute);
      RangeVolumeConsolidator rvConsolidator = getRangeVolumeConsolidator("rvConsolidator", dag);
      ConsoleOutputOperator minuteConsole = getConsole("timedConsole", dag);
      AverageKeyVal<String> priceAvg = getPriceAverage("priceAvg", dag);
      AverageKeyVal<String> volumeAvg = getVolumeAverage("volumeAvg", dag);
      SimpleMovingAverage<String, Double> priceSMA = getPriceSimpleMovingAverage("smaPrice", dag, appWindowCountSMA);
      SimpleMovingAverage<String, Long> volumeSMA = getVolumeSimpleMovingAverage("smaVolume", dag, appWindowCountSMA);
      SMAConsolidator smaConsolidator = getSMAConsolidator("consolidator", dag);
      ConsoleOutputOperator smaConsole = getConsole("smaConsole", dag);

      //dag.addStream("price_tick", tick.price, pvConsolidator.data1, highlow.data, priceAvg.data).setInline(allInline);  // setting output with unchecked type doesn't work. TBD
      //dag.addStream("volume_tick", tick.volume, dailyVolume.data, minuteVolume.data, volumeAvg.data).setInline(allInline);

      dag.addStream("price_tick", tick.price, pvConsolidator.data1, highlow.data).setInline(allInline).addSink(priceAvg.data);
      dag.addStream("volume_tick", tick.volume, dailyVolume.data, minuteVolume.data).setInline(allInline).addSink(volumeAvg.data);
      dag.addStream("time_tick", tick.time, pvConsolidator.data3).setInline(allInline);

      dag.addStream("volume_pvConsolidator", dailyVolume.sum, pvConsolidator.data2).setInline(shouldbeInline);
      dag.addStream("pvConsolidator_console", pvConsolidator.out, windowedConsole.input).setInline(shouldbeInline);

      dag.addStream("highlow_merge", highlow.range, rvConsolidator.data1).setInline(shouldbeInline);
      dag.addStream("volume_merge", minuteVolume.sum, rvConsolidator.data2).setInline(shouldbeInline);
      dag.addStream("minute_console", rvConsolidator.out, minuteConsole.input).setInline(shouldbeInline);

      dag.addStream("priceAverage_priceSma", priceAvg.doubleAverage, priceSMA.data).setInline(shouldbeInline);
      dag.addStream("priceSma_smaCconsolidator", priceSMA.doubleSMA, smaConsolidator.data1).setInline(shouldbeInline);

      dag.addStream("volumeAverage_volumeSma", volumeAvg.longAverage, volumeSMA.data).setInline(shouldbeInline);
      dag.addStream("volumeSma_smaCconsolidator", volumeSMA.longSMA, smaConsolidator.data2).setInline(shouldbeInline);
      dag.addStream("smaConsolidator_console", smaConsolidator.out, smaConsole.input).setInline(shouldbeInline);
    }
    else {
      // nothing
    }

    return dag;
  }
}
