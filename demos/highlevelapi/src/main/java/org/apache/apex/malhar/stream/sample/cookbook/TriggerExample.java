/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.stream.sample.cookbook;

import java.util.Date;
import java.util.Objects;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.CompositeStreamTransform;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;

import com.datatorrent.lib.util.KeyValPair;

/**
 * This example illustrates the basic concepts behind triggering. It shows how to use different
 * trigger definitions to produce partial (speculative) results before all the data is processed and
 * to control when updated results are produced for late data. The example performs a streaming
 * analysis of the data coming in from PubSub and writes the results to BigQuery. It divides the
 * data into {@link Window windows} to be processed, and demonstrates using various kinds of
 * {@link org.apache.beam.sdk.transforms.windowing.Trigger triggers} to control when the results for
 * each window are emitted.
 *
 * <p> This example uses a portion of real traffic data from San Diego freeways. It contains
 * readings from sensor stations set up along each freeway. Each sensor reading includes a
 * calculation of the 'total flow' across all lanes in that freeway direction.
 *
 * <p> Concepts:
 * <pre>
 *   1. The default triggering behavior
 *   2. Late data with the default trigger
 *   3. How to get speculative estimates
 *   4. Combining late data and speculative estimates
 * </pre>
 *
 * <p> Before running this example, it will be useful to familiarize yourself with Dataflow triggers
 * and understand the concept of 'late data',
 * See:  <a href="https://cloud.google.com/dataflow/model/triggers">
 * https://cloud.google.com/dataflow/model/triggers </a> and
 * <a href="https://cloud.google.com/dataflow/model/windowing#Advanced">
 * https://cloud.google.com/dataflow/model/windowing#Advanced </a>
 *
 * <p> The example pipeline reads data from a Pub/Sub topic. By default, running the example will
 * also run an auxiliary pipeline to inject data from the default {@code --input} file to the
 * {@code --pubsubTopic}. The auxiliary pipeline puts a timestamp on the injected data so that the
 * example pipeline can operate on <i>event time</i> (rather than arrival time). The auxiliary
 * pipeline also randomly simulates late data, by setting the timestamps of some of the data
 * elements to be in the past. You may override the default {@code --input} with the file of your
 * choosing or set {@code --input=""} which will disable the automatic Pub/Sub injection, and allow
 * you to use a separate tool to publish to the given topic.
 *
 * <p> The example is configured to use the default Pub/Sub topic and the default BigQuery table
 * from the example common package (there are no defaults for a general Dataflow pipeline).
 * You can override them by using the {@code --pubsubTopic}, {@code --bigQueryDataset}, and
 * {@code --bigQueryTable} options. If the Pub/Sub topic or the BigQuery table do not exist,
 * the example will try to create them.
 *
 * <p> The pipeline outputs its results to a BigQuery table.
 * Here are some queries you can use to see interesting results:
 * Replace {@code <enter_table_name>} in the query below with the name of the BigQuery table.
 * Replace {@code <enter_window_interval>} in the query below with the window interval.
 *
 * <p> To see the results of the default trigger,
 * Note: When you start up your pipeline, you'll initially see results from 'late' data. Wait after
 * the window duration, until the first pane of non-late data has been emitted, to see more
 * interesting results.
 * {@code SELECT * FROM enter_table_name WHERE trigger_type = "default" ORDER BY window DESC}
 *
 * <p> To see the late data i.e. dropped by the default trigger,
 * {@code SELECT * FROM <enter_table_name> WHERE trigger_type = "withAllowedLateness" and
 * (timing = "LATE" or timing = "ON_TIME") and freeway = "5" ORDER BY window DESC, processing_time}
 *
 * <p>To see the the difference between accumulation mode and discarding mode,
 * {@code SELECT * FROM <enter_table_name> WHERE (timing = "LATE" or timing = "ON_TIME") AND
 * (trigger_type = "withAllowedLateness" or trigger_type = "sequential") and freeway = "5" ORDER BY
 * window DESC, processing_time}
 *
 * <p> To see speculative results every minute,
 * {@code SELECT * FROM <enter_table_name> WHERE trigger_type = "speculative" and freeway = "5"
 * ORDER BY window DESC, processing_time}
 *
 * <p> To see speculative results every five minutes after the end of the window
 * {@code SELECT * FROM <enter_table_name> WHERE trigger_type = "sequential" and timing != "EARLY"
 * and freeway = "5" ORDER BY window DESC, processing_time}
 *
 * <p> To see the first and the last pane for a freeway in a window for all the trigger types,
 * {@code SELECT * FROM <enter_table_name> WHERE (isFirst = true or isLast = true) ORDER BY window}
 *
 * <p> To reduce the number of results for each query we can add additional where clauses.
 * For examples, To see the results of the default trigger,
 * {@code SELECT * FROM <enter_table_name> WHERE trigger_type = "default" AND freeway = "5" AND
 * window = "<enter_window_interval>"}
 *
 * <p> The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
 * and then exits.
 */

public class TriggerExample
{
  //Numeric value of fixed window duration, in minutes
  public static final int WINDOW_DURATION = 30;
  // Constants used in triggers.
  // Speeding up ONE_MINUTE or FIVE_MINUTES helps you get an early approximation of results.
  // ONE_MINUTE is used only with processing time before the end of the window
  public static final Duration ONE_MINUTE = Duration.standardMinutes(1);
  // FIVE_MINUTES is used only with processing time after the end of the window
  public static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
  // ONE_DAY is used to specify the amount of lateness allowed for the data elements.
  public static final Duration ONE_DAY = Duration.standardDays(1);

  /**
   * This transform demonstrates using triggers to control when data is produced for each window
   * Consider an example to understand the results generated by each type of trigger.
   * The example uses "freeway" as the key. Event time is the timestamp associated with the data
   * element and processing time is the time when the data element gets processed in the pipeline.
   * For freeway 5, suppose there are 10 elements in the [10:00:00, 10:30:00) window.
   * Key (freeway) | Value (total_flow) | event time | processing time
   * 5             | 50                 | 10:00:03   | 10:00:47
   * 5             | 30                 | 10:01:00   | 10:01:03
   * 5             | 30                 | 10:02:00   | 11:07:00
   * 5             | 20                 | 10:04:10   | 10:05:15
   * 5             | 60                 | 10:05:00   | 11:03:00
   * 5             | 20                 | 10:05:01   | 11.07:30
   * 5             | 60                 | 10:15:00   | 10:27:15
   * 5             | 40                 | 10:26:40   | 10:26:43
   * 5             | 60                 | 10:27:20   | 10:27:25
   * 5             | 60                 | 10:29:00   | 11:11:00
   *
   * <p> Dataflow tracks a watermark which records up to what point in event time the data is
   * complete. For the purposes of the example, we'll assume the watermark is approximately 15m
   * behind the current processing time. In practice, the actual value would vary over time based
   * on the systems knowledge of the current PubSub delay and contents of the backlog (data
   * that has not yet been processed).
   *
   * <p> If the watermark is 15m behind, then the window [10:00:00, 10:30:00) (in event time) would
   * close at 10:44:59, when the watermark passes 10:30:00.
   */
  static class CalculateTotalFlow
      extends CompositeStreamTransform<String, SampleBean>
  {
    private int windowDuration;

    CalculateTotalFlow(int windowDuration)
    {
      this.windowDuration = windowDuration;
    }

    @Override
    public ApexStream<SampleBean> compose(ApexStream<String> inputStream)
    {
      // Concept #1: The default triggering behavior
      // By default Dataflow uses a trigger which fires when the watermark has passed the end of the
      // window. This would be written {@code Repeatedly.forever(AfterWatermark.pastEndOfWindow())}.

      // The system also defaults to dropping late data -- data which arrives after the watermark
      // has passed the event timestamp of the arriving element. This means that the default trigger
      // will only fire once.

      // Each pane produced by the default trigger with no allowed lateness will be the first and
      // last pane in the window, and will be ON_TIME.

      // The results for the example above with the default trigger and zero allowed lateness
      // would be:
      // Key (freeway) | Value (total_flow) | number_of_records | isFirst | isLast | timing
      // 5             | 260                | 6                 | true    | true   | ON_TIME

      // At 11:03:00 (processing time) the system watermark may have advanced to 10:54:00. As a
      // result, when the data record with event time 10:05:00 arrives at 11:03:00, it is considered
      // late, and dropped.

      ApexStream<SampleBean> defaultTriggerResults = inputStream
          .window(new WindowOption.TimeWindows(Duration.standardMinutes(windowDuration)),
          new TriggerOption().discardingFiredPanes())
          .addCompositeStreams(new TotalFlow("default"));

      // Concept #2: Late data with the default trigger
      // This uses the same trigger as concept #1, but allows data that is up to ONE_DAY late. This
      // leads to each window staying open for ONE_DAY after the watermark has passed the end of the
      // window. Any late data will result in an additional pane being fired for that same window.

      // The first pane produced will be ON_TIME and the remaining panes will be LATE.
      // To definitely get the last pane when the window closes, use
      // .withAllowedLateness(ONE_DAY, ClosingBehavior.FIRE_ALWAYS).

      // The results for the example above with the default trigger and ONE_DAY allowed lateness
      // would be:
      // Key (freeway) | Value (total_flow) | number_of_records | isFirst | isLast | timing
      // 5             | 260                | 6                 | true    | false  | ON_TIME
      // 5             | 60                 | 1                 | false   | false  | LATE
      // 5             | 30                 | 1                 | false   | false  | LATE
      // 5             | 20                 | 1                 | false   | false  | LATE
      // 5             | 60                 | 1                 | false   | false  | LATE
      ApexStream<SampleBean> withAllowedLatenessResults = inputStream
          .window(new WindowOption.TimeWindows(Duration.standardMinutes(windowDuration)),
          new TriggerOption().discardingFiredPanes(),
          Duration.standardDays(1))
          .addCompositeStreams(new TotalFlow("withAllowedLateness"));

      // Concept #3: How to get speculative estimates
      // We can specify a trigger that fires independent of the watermark, for instance after
      // ONE_MINUTE of processing time. This allows us to produce speculative estimates before
      // all the data is available. Since we don't have any triggers that depend on the watermark
      // we don't get an ON_TIME firing. Instead, all panes are either EARLY or LATE.

      // We also use accumulatingFiredPanes to build up the results across each pane firing.

      // The results for the example above for this trigger would be:
      // Key (freeway) | Value (total_flow) | number_of_records | isFirst | isLast | timing
      // 5             | 80                 | 2                 | true    | false  | EARLY
      // 5             | 100                | 3                 | false   | false  | EARLY
      // 5             | 260                | 6                 | false   | false  | EARLY
      // 5             | 320                | 7                 | false   | false  | LATE
      // 5             | 370                | 9                 | false   | false  | LATE
      // 5             | 430                | 10                | false   | false  | LATE

      ApexStream<SampleBean> speculativeResults = inputStream
          .window(new WindowOption.TimeWindows(Duration.standardMinutes(windowDuration)),
              //Trigger fires every minute
          new TriggerOption().withEarlyFiringsAtEvery(Duration.standardMinutes(1))
                  // After emitting each pane, it will continue accumulating the elements so that each
                  // approximation includes all of the previous data in addition to the newly arrived
                  // data.
          .accumulatingFiredPanes(),
          Duration.standardDays(1))
          .addCompositeStreams(new TotalFlow("speculative"));

      // Concept #4: Combining late data and speculative estimates
      // We can put the previous concepts together to get EARLY estimates, an ON_TIME result,
      // and LATE updates based on late data.

      // Each time a triggering condition is satisfied it advances to the next trigger.
      // If there are new elements this trigger emits a window under following condition:
      // > Early approximations every minute till the end of the window.
      // > An on-time firing when the watermark has passed the end of the window
      // > Every five minutes of late data.

      // Every pane produced will either be EARLY, ON_TIME or LATE.

      // The results for the example above for this trigger would be:
      // Key (freeway) | Value (total_flow) | number_of_records | isFirst | isLast | timing
      // 5             | 80                 | 2                 | true    | false  | EARLY
      // 5             | 100                | 3                 | false   | false  | EARLY
      // 5             | 260                | 6                 | false   | false  | EARLY
      // [First pane fired after the end of the window]
      // 5             | 320                | 7                 | false   | false  | ON_TIME
      // 5             | 430                | 10                | false   | false  | LATE

      // For more possibilities of how to build advanced triggers, see {@link Trigger}.
      ApexStream<SampleBean> sequentialResults = inputStream
          .window(new WindowOption.TimeWindows(Duration.standardMinutes(windowDuration)),
              // Speculative every ONE_MINUTE
          new TriggerOption().withEarlyFiringsAtEvery(Duration.standardMinutes(1))
          .withLateFiringsAtEvery(Duration.standardMinutes(5))
                  // After emitting each pane, it will continue accumulating the elements so that each
                  // approximation includes all of the previous data in addition to the newly arrived
                  // data.
          .accumulatingFiredPanes(),
          Duration.standardDays(1))
          .addCompositeStreams(new TotalFlow("sequential"));

      return sequentialResults;
    }

  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // The remaining parts of the pipeline are needed to produce the output for each
  // concept above. Not directly relevant to understanding the trigger examples.

  /**
   * Calculate total flow and number of records for each freeway and format the results to TableRow
   * objects, to save to BigQuery.
   */
  static class TotalFlow extends
      CompositeStreamTransform<String, SampleBean>
  {
    private String triggerType;

    public TotalFlow(String triggerType)
    {
      this.triggerType = triggerType;
    }

    @Override
    public ApexStream<SampleBean> compose(ApexStream<String> inputStream)
    {
      if (!(inputStream instanceof WindowedStream)) {
        throw new RuntimeException("Not supported here");
      }
      WindowedStream<String> windowedStream = (WindowedStream<String>)inputStream;
      ApexStream<KeyValPair<String, Iterable<Integer>>> flowPerFreeway = windowedStream
          .groupByKey(new ExtractFlowInfo());

      return flowPerFreeway
          .map(new Function.MapFunction<KeyValPair<String, Iterable<Integer>>, KeyValPair<String, String>>()
          {
            @Override
            public KeyValPair<String, String> f(KeyValPair<String, Iterable<Integer>> input)
            {
              Iterable<Integer> flows = input.getValue();
              Integer sum = 0;
              Long numberOfRecords = 0L;
              for (Integer value : flows) {
                sum += value;
                numberOfRecords++;
              }
              return new KeyValPair<>(input.getKey(), sum + "," + numberOfRecords);
            }
          })
          .map(new FormatTotalFlow(triggerType));
    }
  }

  /**
   * Format the results of the Total flow calculation to a TableRow, to save to BigQuery.
   * Adds the triggerType, pane information, processing time and the window timestamp.
   */
  static class FormatTotalFlow implements Function.MapFunction<KeyValPair<String, String>, SampleBean>
  {
    private String triggerType;

    public FormatTotalFlow(String triggerType)
    {
      this.triggerType = triggerType;
    }

    @Override
    public SampleBean f(KeyValPair<String, String> input)
    {
      String[] values = input.getValue().split(",");
      //TODO need to have a callback to get the metadata like window id, pane id, timestamps etc.
      return new SampleBean(triggerType, input.getKey(), Integer.parseInt(values[0]), Long
          .parseLong(values[1]), null, false, false, null, null, new Date());
    }
  }

  public static class SampleBean
  {
    public SampleBean()
    {
    }

    private String trigger_type;

    private String freeway;

    private int total_flow;

    private long number_of_records;

    private String window;

    private boolean isFirst;

    private boolean isLast;

    private Date timing;

    private Date event_time;

    private Date processing_time;

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SampleBean that = (SampleBean)o;
      return total_flow == that.total_flow &&
          number_of_records == that.number_of_records &&
          isFirst == that.isFirst &&
          isLast == that.isLast &&
          Objects.equals(trigger_type, that.trigger_type) &&
          Objects.equals(freeway, that.freeway) &&
          Objects.equals(window, that.window) &&
          Objects.equals(timing, that.timing) &&
          Objects.equals(event_time, that.event_time) &&
          Objects.equals(processing_time, that.processing_time);
    }

    @Override
    public int hashCode()
    {
      return Objects
          .hash(trigger_type, freeway, total_flow, number_of_records, window, isFirst, isLast, timing, event_time,
              processing_time);
    }

    public SampleBean(String trigger_type, String freeway, int total_flow, long number_of_records, String window,
        boolean isFirst, boolean isLast, Date timing, Date event_time, Date processing_time)
    {

      this.trigger_type = trigger_type;
      this.freeway = freeway;
      this.total_flow = total_flow;
      this.number_of_records = number_of_records;
      this.window = window;
      this.isFirst = isFirst;
      this.isLast = isLast;
      this.timing = timing;
      this.event_time = event_time;
      this.processing_time = processing_time;
    }

    public String getTrigger_type()
    {
      return trigger_type;
    }

    public void setTrigger_type(String trigger_type)
    {
      this.trigger_type = trigger_type;
    }

    public String getFreeway()
    {
      return freeway;
    }

    public void setFreeway(String freeway)
    {
      this.freeway = freeway;
    }

    public int getTotal_flow()
    {
      return total_flow;
    }

    public void setTotal_flow(int total_flow)
    {
      this.total_flow = total_flow;
    }

    public long getNumber_of_records()
    {
      return number_of_records;
    }

    public void setNumber_of_records(long number_of_records)
    {
      this.number_of_records = number_of_records;
    }

    public String getWindow()
    {
      return window;
    }

    public void setWindow(String window)
    {
      this.window = window;
    }

    public boolean isFirst()
    {
      return isFirst;
    }

    public void setFirst(boolean first)
    {
      isFirst = first;
    }

    public boolean isLast()
    {
      return isLast;
    }

    public void setLast(boolean last)
    {
      isLast = last;
    }

    public Date getTiming()
    {
      return timing;
    }

    public void setTiming(Date timing)
    {
      this.timing = timing;
    }

    public Date getEvent_time()
    {
      return event_time;
    }

    public void setEvent_time(Date event_time)
    {
      this.event_time = event_time;
    }

    public Date getProcessing_time()
    {
      return processing_time;
    }

    public void setProcessing_time(Date processing_time)
    {
      this.processing_time = processing_time;
    }
  }

  /**
   * Extract the freeway and total flow in a reading.
   * Freeway is used as key since we are calculating the total flow for each freeway.
   */
  static class ExtractFlowInfo implements Function.MapFunction<String, KeyValPair<String, Integer>>
  {
    @Override
    public KeyValPair<String, Integer> f(String input)
    {
      String[] laneInfo = input.split(",");
      if (laneInfo[0].equals("timestamp")) {
        // Header row
        return null;
      }
      if (laneInfo.length < 48) {
        //Skip the invalid input.
        return null;
      }
      String freeway = laneInfo[2];
      Integer totalFlow = tryIntegerParse(laneInfo[7]);
      // Ignore the records with total flow 0 to easily understand the working of triggers.
      // Skip the records with total flow -1 since they are invalid input.
      if (totalFlow == null || totalFlow <= 0) {
        return null;
      }
      return new KeyValPair<>(freeway, totalFlow);
    }
  }

  private static final String PUBSUB_TIMESTAMP_LABEL_KEY = "timestamp_ms";

  public static void main(String[] args) throws Exception
  {
    StreamFactory.fromFolder("some folder")
        .addCompositeStreams(new CalculateTotalFlow(60));

  }

  private static Integer tryIntegerParse(String number)
  {
    try {
      return Integer.parseInt(number);
    } catch (NumberFormatException e) {
      return null;
    }
  }

}
