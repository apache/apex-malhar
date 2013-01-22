/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.demos.samplestream.YahooFinanceCSVSpout;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.math.AverageKeyVal;
import com.malhartech.lib.multiwindow.MultiWindowRangeKeyVal;
import com.malhartech.lib.multiwindow.MultiWindowSumKeyVal;
import com.malhartech.lib.multiwindow.SimpleMovingAverage;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class YahooFinanceApplication implements ApplicationFactory
{
  private static final Logger logger = LoggerFactory.getLogger(YahooFinanceApplication.class);

  public YahooFinanceCSVSpout getYahooFinanceTick(String name, DAG dag)
  {
    // url: http://download.finance.yahoo.com/d/quotes.csv?s=GOOG,FB,YHOO&f=sl1vt1&e=.csv
    YahooFinanceCSVSpout oper = dag.addOperator(name, YahooFinanceCSVSpout.class);
    oper.addSymbol("GOOG");
    oper.addSymbol("FB");
    oper.addSymbol("YHOO");

    oper.addFormat(YahooFinanceCSVSpout.Symbol);
    oper.addFormat(YahooFinanceCSVSpout.LastTrade);
    oper.addFormat(YahooFinanceCSVSpout.Volume);
    oper.addFormat(YahooFinanceCSVSpout.LastTradeTime);

    return oper;
  }


  public StockTickInput getTicks(String name, DAG dag)
  {
    StockTickInput oper = dag.addOperator(name, StockTickInput.class);

    return oper;
  }

  public DummyStockTickInput getDummyStockTickInput(String name, DAG dag)
  {
    DummyStockTickInput oper = dag.addOperator(name, DummyStockTickInput.class);

    return oper;
  }

  public DailyVolume getDailyVolume(String name, DAG dag)
  {
    DailyVolume oper = dag.addOperator(name, DailyVolume.class);
    oper.setType(Long.class);
    return oper;
  }

  public MultiWindowSumKeyVal<String, Long> getTimedVolume(String name, DAG dag, int appWindow)
  {
    MultiWindowSumKeyVal<String, Long> oper = dag.addOperator(name, MultiWindowSumKeyVal.class);
    oper.setType(Long.class);
    oper.setWindowSize(appWindow);
    return oper;
  }

  public MultiWindowRangeKeyVal<String, Double> getTimedPriceRange(String name, DAG dag, int appWindow)
  {
    MultiWindowRangeKeyVal<String, Double> oper = dag.addOperator(name, MultiWindowRangeKeyVal.class);
    oper.setType(Double.class);
    oper.setWindowSize(appWindow);
    return oper;
  }

  public PriceVolumeConsolidator getPriceVolumeConsolidator(String name, DAG dag)
  {
    PriceVolumeConsolidator oper = dag.addOperator(name, PriceVolumeConsolidator.class);

    return oper;
  }

  public RangeVolumeConsolidator getRangeVolumeConsolidator(String name, DAG dag)
  {
    RangeVolumeConsolidator oper = dag.addOperator(name, RangeVolumeConsolidator.class);

    return oper;
  }

  public AverageKeyVal<String, Double> getPriceAverage(String name, DAG dag)
  {
    AverageKeyVal<String, Double> oper = dag.addOperator(name, AverageKeyVal.class);

    return oper;
  }

  public SimpleMovingAverage getSimpleMovingAverage(String name, DAG dag, int appWindow)
  {
    SimpleMovingAverage oper = dag.addOperator(name, SimpleMovingAverage.class);
    oper.setWindowSize(appWindow);
    oper.setType(Double.class);
    return oper;
  }

  public ConsoleOutputOperator getConsole(String name, DAG dag)
  {
    ConsoleOutputOperator oper = dag.addOperator(name, ConsoleOutputOperator.class);

    return oper;
  }

  @Override
  public DAG getApplication(Configuration conf)
  {
    boolean allInline = true;
    DAG dag = new DAG(conf);
    // Default streaming window size is 500 msec. Set this to 1 sec.
    int streamingWindowSize = 1000;
    dag.getAttributes().attr(DAG.STRAM_WINDOW_SIZE_MILLIS).set(streamingWindowSize);
    boolean windowTest = false;
    boolean timeTest = false;
    boolean smatest = false;
    boolean financetick = true;

    DummyStockTickInput tick = getDummyStockTickInput("tick", dag);
    if (windowTest) {
      DailyVolume windowedVolume = getDailyVolume("windowedVolume", dag);
      PriceVolumeConsolidator priceVolumeConsolidator = getPriceVolumeConsolidator("priceVolumeMerger", dag);
      ConsoleOutputOperator windowedConsole = getConsole("windowedConsole", dag);

      dag.addStream("volume_tick", tick.volume, windowedVolume.data).setInline(allInline);
      dag.addStream("price_tick", tick.price, priceVolumeConsolidator.data1).setInline(allInline);
      dag.addStream("time_tick", tick.time, priceVolumeConsolidator.data3).setInline(allInline);

      dag.addStream("totalVolume", windowedVolume.sum, priceVolumeConsolidator.data2).setInline(true);
      dag.addStream("count", windowedVolume.count, priceVolumeConsolidator.data4).setInline(true); // only for testing

      dag.addStream("windowed_console", priceVolumeConsolidator.out, windowedConsole.input).setInline(true);
    }
    else if (timeTest) {
      int winSize = 4;
      MultiWindowRangeKeyVal<String, Double> highlow = getTimedPriceRange("highlow", dag, winSize);
      MultiWindowSumKeyVal<String, Long> timedVolume = getTimedVolume("timedVolume", dag, winSize);
      RangeVolumeConsolidator consolidator2 = getRangeVolumeConsolidator("con2", dag);
      ConsoleOutputOperator timedConsole = getConsole("timedConsole", dag);

      dag.addStream("price_tick", tick.price, highlow.data).setInline(allInline);
      dag.addStream("volume_tick", tick.volume, timedVolume.data).setInline(allInline);

      dag.addStream("highlow_merge", highlow.range, consolidator2.data1).setInline(true);
      dag.addStream("volume_merge", timedVolume.sum, consolidator2.data2).setInline(true);
      dag.addStream("average_merge", timedVolume.average, consolidator2.data3).setInline(true); // only for tesing
      dag.addStream("count_merge", timedVolume.count, consolidator2.data4).setInline(true); // only for tesing

      dag.addStream("windowed_console", consolidator2.out, timedConsole.input).setInline(true);
    }
    else if (smatest){ // sma test
      int appWindow = 4;
      AverageKeyVal<String, Double> priceAvg = getPriceAverage("priceAvg", dag);
      ConsoleOutputOperator smaConsole = getConsole("smaConsole", dag);
      SimpleMovingAverage sma = getSimpleMovingAverage("sma", dag, appWindow);

      dag.addStream("price_tick", tick.price, priceAvg.data).setInline(allInline);
      dag.addStream("average_sma", priceAvg.average, sma.data).setInline(allInline);
      dag.addStream("sma_console", sma.doubleSMA, smaConsole.input).setInline(true);
    }
    else if (financetick){
      YahooFinanceCSVSpout tick2 = getYahooFinanceTick("tick2", dag);
      StockTickInput ticks = getTicks("ticks", dag);
      PriceVolumeConsolidator priceVolumeConsolidator = getPriceVolumeConsolidator("priceVolumeMerger", dag);
      ConsoleOutputOperator financeConsole = getConsole("financeConsole", dag);

      dag.addStream("tick2_ticks", tick2.outputPort, ticks.data).setInline(true);
      dag.addStream("ticks_price", ticks.price, priceVolumeConsolidator.data1).setInline(allInline);
      dag.addStream("ticks_volume", ticks.volume, priceVolumeConsolidator.data2).setInline(allInline);
      dag.addStream("ticks_time", ticks.time, priceVolumeConsolidator.data3).setInline(allInline);

      dag.addStream("consolidator_console", priceVolumeConsolidator.out, financeConsole.input).setInline(true);
    }
    else {
      // nothing
    }

    return dag;
  }
}
