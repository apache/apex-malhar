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
package com.datatorrent.demos.yahoofinance;


import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.math.RangeKeyVal;
import com.datatorrent.lib.math.SumKeyVal;
import com.datatorrent.lib.multiwindow.SimpleMovingAverage;
import com.datatorrent.lib.stream.ConsolidatorKeyVal;
import com.datatorrent.lib.util.HighLow;

/**
 * Yahoo! Finance Application Demo :<br>
 * Get Yahoo finance feed and calculate minute price range, minute volume,
 * simple moving average of 5 minutes. <br>
 * <br>
 * Functional Description : <br>
 * Application samples yahoo finance ticker every 200ms. All data points in one
 * second are streamed from input adapter. <br>
 * <br>
 *
 * Application calculates following Real Time Value(s):<br>
 * <ul>
 * <li>Quotes for IBM, Google, Apple, Yahoo stocks price/volume/time displayed
 * every second.</li>
 * <li>Charts for Stocks in terms for high/low price vs volume for last minute.</li>
 * <li>Simple moving average over last 5 minutes for IBM, Google, Apple, Yahoo
 * stocks.</li>
 * </ul>
 * <br>
 * <br>
 *
 * Custom Attribute : <br>
 * <ul>
 * <li>Application streaming window size(STREAMING_WINDOW_SIZE_MILLIS) = 1 sec,
 * since we are only interested in quotes every second.</li>
 * <li>Range/Minute Volume operator's window size(APPLICATION_WINDOW_COUNT) =
 * 60, aggregate over one minute.</li>
 * <li>Sum operator window length : 300, sliding average over last 5 minutes.</li>
 * </ul>
 * <br>
 *
 * Input Adapter : <br>
 * Stock Tick input operator get yahoo finance real time stock quotes data and
 * pushes application. <br>
 * <br>
 *
 * Output Adapter : <br>
 * Output values are written to console through ConsoleOutputOerator<br>
 * if you need to change write to HDFS,HTTP .. instead of console, <br>
 * Please refer to {@link com.datatorrent.lib.io.HttpOutputOperator} or
 * {@link com.datatorrent.lib.io.fs.HdfsOutputOperator}. <br>
 * <br>
 *
 * Run Sample Application : <br>
 * <p>
 * Running Java Test or Main app in IDE:
 *
 * <pre>
 * LocalMode.runApp(new Application(), 600000); // 10 min run
 * </pre>
 *
 * Run Success : <br>
 * For successful deployment and run, user should see following output on
 * console:
 *
 * <pre>
 *  Price SMA: AAPL=435.965
 * Price SMA: GOOG=877.0
 * QUOTE: {YHOO=[26.37, 9760360, 4:00pm, null, null], IBM=[203.77, 2899698, 4:00pm, null, null], GOOG=[877.0, 2069614, 4:00pm, null, null], AAPL=[435.965, 10208099, 4:00pm, null, null]}
 * Price SMA: YHOO=26.37
 * </pre>
 *
 * Scaling Options : <br>
 * <ul>
 * <li>Volume operator can be replicated using 'INITIAL_PARTITION_COUNT' options
 * on operator.</li>
 * <li>Range value operator can replicated but using proper unifier
 * operator(read App Dev Guide).</li>
 * <li>Slinging window operator can be replicated with proper unifier operator.</li>
 * </ul>
 * <br>
 *
 * Application DAG : <br>
 * <img src="doc-files/Application.gif" width=600px > <br>
 * <br>
 *
 * Streaming Window Size : 1000 ms(1 Sec) <br>
 * Operator Details : <br>
 * <ul>
 * <li>
 * <p>
 * <b>The operator DailyVolume:</b> This operator reads from the input port,
 * which contains the incremental volume tuples from StockTickInput, and
 * aggregates the data to provide the cumulative volume. It just utilizes the
 * library class SumKeyVal<K,V> provided in math package. In this case,
 * SumKeyVal<String,Long>, where K is the stock symbol, V is the aggregated
 * volume, with cumulative set to true. (Otherwise if cumulative was set to
 * false, SumKeyVal would provide the sum for the application window.) The platform
 * provides a number of built-in operators for simple operations like this so
 * that application developers do not have to write them. More examples to
 * follow. This operator assumes that the application restarts before market
 * opens every day.
 * </p>
 * Class : {@link com.datatorrent.lib.math.SumKeyVal} <br>
 * Operator Application Window Count : 1 <br>
 * StateFull : Yes, volume gets aggregated every window count.</li>
 *
 * <li>
 * <p>
 * <b>The operator MinuteVolume:</b> This operator reads from the input port,
 * which contains the volume tuples from StockTickInput, and aggregates the data
 * to provide the sum of the volume within one minute. Like the operator
 * DailyVolume, this operator is also SumKeyVal<String,Long>, but with
 * cumulative set to false. Application Window is set to 1 minute. We will
 * explain how to set this later. <br>
 * Class : {@link com.datatorrent.lib.math.SumKeyVal} <br>
 * Operator App Window Count : 60 (1 Minute) <br>
 * StateFull : Yes, aggregate over last 60 windows.</li>
 *
 * <li>
 * <p>
 * <b>The operator Quote:</b> This operator has three input ports, which are
 * price (from StockTickInput), daily_vol (from Daily Volume), and time (from
 * StockTickInput). This operator just consolidates the three data and and emits
 * the consolidated data. It utilizes the class ConsolidatorKeyVal<K> from
 * stream package.<br>
 * Class : {@link com.datatorrent.lib.stream.ConsolidatorKeyVal} <br>
 * Operator App Window Count : 1 <br>
 * StateFull : No</li>
 *
 * <li>
 * <p>
 * <b>The operator Chart:</b> This operator is very similar to the operator
 * Quote, except that it takes inputs from High Low and Minute Vol and outputs
 * the consolidated tuples to the output port. <br>
 * Class : {@link com.datatorrent.lib.stream.ConsolidatorKeyVal} <br>
 * StateFull : No<br>
 * Operator App Window Count : 1</li>
 *
 *
 * <li>
 * <p>
 * <b>The operator PriceSMA:</b> SMA stands for - Simple Moving Average. It
 * reads from the input port, which contains the price tuples from
 * StockTickInput, and provides the moving average price of the stock. It
 * utilizes SimpleMovingAverage<String,Double>, which is provided in multiwindow
 * package. SimpleMovingAverage keeps track of the data of the previous N
 * application windows in a sliding manner. For each end window event, it
 * provides the average of the data in those application windows. <br>
 * Class : {@link com.datatorrent.lib.multiwindow.SimpleMovingAverage} <br>
 * StateFull : Yes, stores values across application window. <br>
 * Operator App Window : 1 <br>
 * Operator Sliding Window : 300 (5 mins).</li>
 *
 * <li>
 * <p>
 * <b>The operator Console: </b> This operator just outputs the input tuples to
 * the console (or stdout). In this example, there are four console operators,
 * which connect to the output of Quote, Chart, PriceSMA and VolumeSMA. In
 * practice, they should be replaced by operators which would do something about
 * the data, like drawing charts. </li>
 *
 * </ul>
 * <br>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name="YahooFinanceApplication")
public class Application implements StreamingApplication
{
  protected int streamingWindowSizeMilliSeconds = 1000; // 1 second
  protected int appWindowCountMinute = 60;   // 1 minute
  protected int appWindowCountSMA = 300;  // 5 minute
  protected String[] tickers = {"IBM", "GOOG", "AAPL", "YHOO"};

  /**
   * Instantiate stock input operator for actual Yahoo finance ticks of symbol, last price, total daily volume, and last traded price.
   * @param name  Operator name
   * @param dag   Application DAG graph.
   * @return StockTickInput instance.
   */
  public StockTickInput getStockTickInputOperator(String name, DAG dag)
  {
    StockTickInput oper = dag.addOperator(name, StockTickInput.class);
    oper.readIntervalMillis = 200;
    oper.symbols = tickers;
    return oper;
  }

  /**
   * Instantiate {@link com.datatorrent.lib.math.SumKeyVal} operator
   * to sends total daily volume by adding volumes from each ticks.
   * @param name  Operator name
   * @param dag   Application DAG graph.
   * @return SumKeyVal instance.
   */
  public SumKeyVal<String, Long> getDailyVolumeOperator(String name, DAG dag)
  {
    SumKeyVal<String, Long> oper = dag.addOperator(name, new SumKeyVal<String, Long>());
    oper.setType(Long.class);
    oper.setCumulative(true);
    return oper;
  }

  /**
   * Instantiate {@link com.datatorrent.lib.math.SumKeyVal} operator
   * Get aggregated volume of 1 minute and send at the end window of 1 minute.
   * @param name  Operator name
   * @param dag   Application DAG graph.
   * @param appWindowCount Operator aggregate window size.
   * @return SumKeyVal instance.
   */
  public SumKeyVal<String, Long> getMinuteVolumeOperator(String name, DAG dag, int appWindowCount)
  {
    SumKeyVal<String, Long> oper = dag.addOperator(name, new SumKeyVal<String, Long>());
    oper.setType(Long.class);
    dag.getOperatorMeta(name).getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, appWindowCount);
    return oper;
  }

  /**
   * Instantiate {@link com.datatorrent.lib.math.RangeKeyVal} operator to get high/low
   * value for each key within given application window.
   * Get High-low range for 1 minute.
   * @param name  Operator name
   * @param dag   Application DAG graph.
   * @param appWindowCount Operator aggregate window size.
   * @return RangeKeyVal instance.
   */
  public RangeKeyVal<String, Double> getHighLowOperator(String name, DAG dag, int appWindowCount)
  {
    RangeKeyVal<String, Double> oper = dag.addOperator(name, new RangeKeyVal<String, Double>());
    dag.getOperatorMeta(name).getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, appWindowCount);
    oper.setType(Double.class);
    return oper;
  }

  /**
   * Instantiate {@link com.datatorrent.lib.stream.ConsolidatorKeyVal} to send
   * Quote (Merge price, daily volume, time)
   * @param name  Operator name
   * @param dag   Application DAG graph.
   * @return ConsolidatorKeyVal instance.
   */
  public ConsolidatorKeyVal<String,Double,Long,String,?,?> getQuoteOperator(String name, DAG dag)
  {
    ConsolidatorKeyVal<String,Double,Long,String,?,?> oper = dag.addOperator(name, new ConsolidatorKeyVal<String,Double,Long,String,Object,Object>());
    return oper;
  }

  /**
   * Instantiate {@link com.datatorrent.lib.stream.ConsolidatorKeyVal} to send
   * Chart (Merge minute volume and minute high-low)
   * @param name  Operator name
   * @param dag   Application DAG graph.
   * @return ConsolidatorKeyVal instance.
   */
  public ConsolidatorKeyVal<String,HighLow<Double>,Long,?,?,?> getChartOperator(String name, DAG dag)
  {
    ConsolidatorKeyVal<String,HighLow<Double>,Long,?,?,?> oper = dag.addOperator(name, new ConsolidatorKeyVal<String,HighLow<Double>,Long,Object,Object,Object>());
    return oper;
  }

  /**
   * Instantiate {@link com.datatorrent.lib.multiwindow.SimpleMovingAverage} to calculate moving average for price
   * over given window size. Sliding window size is 1.
   * @param name  Operator name
   * @param dag   Application DAG graph.
   * @param appWindowCount Operator aggregate window size.
   * @return SimpleMovingAverage instance.
   */
  public SimpleMovingAverage<String, Double> getPriceSimpleMovingAverageOperator(String name, DAG dag, int appWindowCount)
  {
    SimpleMovingAverage<String, Double> oper = dag.addOperator(name, new SimpleMovingAverage<String, Double>());
    oper.setWindowSize(appWindowCount);
    oper.setType(Double.class);
    return oper;
  }

  /**
   * Get console for output operator.
   * @param name  Operator name
   * @param dag   Application DAG graph.
   * @return input port for console output.
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
   * Populate Yahoo Finance Demo Application DAG.
   */
	@SuppressWarnings("unchecked")
	@Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    dag.getAttributes().put(DAG.STREAMING_WINDOW_SIZE_MILLIS, streamingWindowSizeMilliSeconds);

    StockTickInput tick = getStockTickInputOperator("StockTickInput", dag);
    SumKeyVal<String, Long> dailyVolume = getDailyVolumeOperator("DailyVolume", dag);
    ConsolidatorKeyVal<String,Double,Long,String,?,?> quoteOperator = getQuoteOperator("Quote", dag);

    RangeKeyVal<String, Double> highlow = getHighLowOperator("HighLow", dag, appWindowCountMinute);
    SumKeyVal<String, Long> minuteVolume = getMinuteVolumeOperator("MinuteVolume", dag, appWindowCountMinute);
    ConsolidatorKeyVal<String,HighLow<Double>,Long,?,?,?> chartOperator = getChartOperator("Chart", dag);

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
