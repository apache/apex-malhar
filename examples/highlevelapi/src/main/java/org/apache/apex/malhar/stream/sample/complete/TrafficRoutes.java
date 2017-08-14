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
package org.apache.apex.malhar.stream.sample.complete;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.accumulation.Group;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.CompositeStreamTransform;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;

import static org.apache.apex.malhar.stream.api.Option.Options.name;

/**
 * Beam's TrafficRoutes example.
 *
 * @since 3.5.0
 */
@ApplicationAnnotation(name = "TrafficRoutes")
public class TrafficRoutes implements StreamingApplication
{
  static Map<String, String> sdStations = buildStationInfo();
  static final int WINDOW_DURATION = 3;  // Default sliding window duration in minutes
  static final int WINDOW_SLIDE_EVERY = 1;  // Default window 'slide every' setting in minutes

  /**
   * This class holds information about a station reading's average speed.
   */
  public static class StationSpeed implements Comparable<StationSpeed>
  {
    @Nullable
    String stationId;
    @Nullable
    Double avgSpeed;
    @Nullable
    Long timestamp;

    public StationSpeed() {}

    public StationSpeed(String stationId, Double avgSpeed, Long timestamp)
    {
      this.stationId = stationId;
      this.avgSpeed = avgSpeed;
      this.timestamp = timestamp;
    }

    public void setAvgSpeed(@Nullable Double avgSpeed)
    {
      this.avgSpeed = avgSpeed;
    }

    public void setStationId(@Nullable String stationId)
    {
      this.stationId = stationId;
    }

    public void setTimestamp(@Nullable Long timestamp)
    {
      this.timestamp = timestamp;
    }

    @Nullable
    public Long getTimestamp()
    {
      return timestamp;
    }

    public String getStationId()
    {
      return this.stationId;
    }

    public Double getAvgSpeed()
    {
      return this.avgSpeed;
    }

    @Override
    public int compareTo(StationSpeed other)
    {
      return Long.compare(this.timestamp, other.timestamp);
    }
  }

  /**
   * This class holds information about a route's speed/slowdown.
   */
  static class RouteInfo
  {
    @Nullable
    String route;
    @Nullable
    Double avgSpeed;
    @Nullable
    Boolean slowdownEvent;

    public RouteInfo()
    {

    }

    public RouteInfo(String route, Double avgSpeed, Boolean slowdownEvent)
    {
      this.route = route;
      this.avgSpeed = avgSpeed;
      this.slowdownEvent = slowdownEvent;
    }

    public String getRoute()
    {
      return this.route;
    }

    public Double getAvgSpeed()
    {
      return this.avgSpeed;
    }

    public Boolean getSlowdownEvent()
    {
      return this.slowdownEvent;
    }
  }

  /**
   * Extract the timestamp field from the input string, and wrap the input string in a {@link Tuple.TimestampedTuple}
   * with the extracted timestamp.
   */
  static class ExtractTimestamps implements Function.MapFunction<String, Tuple.TimestampedTuple<String>>
  {

    @Override
    public Tuple.TimestampedTuple<String> f(String input)
    {
      String[] items = input.split(",");
      String timestamp = tryParseTimestamp(items);

      return new Tuple.TimestampedTuple<>(Long.parseLong(timestamp), input);
    }
  }

  /**
   * Filter out readings for the stations along predefined 'routes', and output
   * (station, speed info) keyed on route.
   */
  static class ExtractStationSpeedFn implements Function.FlatMapFunction<Tuple.TimestampedTuple<String>, KeyValPair<String, StationSpeed>>
  {

    @Override
    public Iterable<KeyValPair<String, StationSpeed>> f(Tuple.TimestampedTuple<String> input)
    {

      ArrayList<KeyValPair<String, StationSpeed>> result = new ArrayList<>();
      String[] items = input.getValue().split(",");
      String stationType = tryParseStationType(items);
      // For this analysis, use only 'main line' station types
      if (stationType != null && stationType.equals("ML")) {
        Double avgSpeed = tryParseAvgSpeed(items);
        String stationId = tryParseStationId(items);
        // For this simple example, filter out everything but some hardwired routes.
        if (avgSpeed != null && stationId != null && sdStations.containsKey(stationId)) {
          StationSpeed stationSpeed =
              new StationSpeed(stationId, avgSpeed, input.getTimestamp());
          // The tuple key is the 'route' name stored in the 'sdStations' hash.
          KeyValPair<String, StationSpeed> outputValue = new KeyValPair<>(sdStations.get(stationId), stationSpeed);
          result.add(outputValue);
        }
      }
      return result;
    }
  }

  /**
   * For a given route, track average speed for the window. Calculate whether
   * traffic is currently slowing down, via a predefined threshold. If a supermajority of
   * speeds in this sliding window are less than the previous reading we call this a 'slowdown'.
   * Note: these calculations are for example purposes only, and are unrealistic and oversimplified.
   */
  static class GatherStats
      implements Function.FlatMapFunction<Tuple.WindowedTuple<KeyValPair<String, List<StationSpeed>>>, Tuple.TimestampedTuple<KeyValPair<String, RouteInfo>>>
  {
    @Override
    public Iterable<Tuple.TimestampedTuple<KeyValPair<String, RouteInfo>>> f(Tuple.WindowedTuple<KeyValPair<String, List<StationSpeed>>> input)
    {
      ArrayList<Tuple.TimestampedTuple<KeyValPair<String, RouteInfo>>> result = new ArrayList<>();
      String route = input.getValue().getKey();
      double speedSum = 0.0;
      int speedCount = 0;
      int speedups = 0;
      int slowdowns = 0;
      List<StationSpeed> infoList = Lists.newArrayList(input.getValue().getValue());
      // StationSpeeds sort by embedded timestamp.
      Collections.sort(infoList);
      Map<String, Double> prevSpeeds = new HashMap<>();
      // For all stations in the route, sum (non-null) speeds. Keep a count of the non-null speeds.
      for (StationSpeed item : infoList) {
        Double speed = item.getAvgSpeed();
        if (speed != null) {
          speedSum += speed;
          speedCount++;
          Double lastSpeed = prevSpeeds.get(item.getStationId());
          if (lastSpeed != null) {
            if (lastSpeed < speed) {
              speedups += 1;
            } else {
              slowdowns += 1;
            }
          }
          prevSpeeds.put(item.getStationId(), speed);
        }
      }
      if (speedCount == 0) {
        // No average to compute.
        return result;
      }
      double speedAvg = speedSum / speedCount;
      boolean slowdownEvent = slowdowns >= 2 * speedups;
      RouteInfo routeInfo = new RouteInfo(route, speedAvg, slowdownEvent);
      result.add(new Tuple.TimestampedTuple<KeyValPair<String, RouteInfo>>(input.getTimestamp(), new KeyValPair<String, RouteInfo>(route, routeInfo)));
      return result;
    }
  }

  /**
   * Output Pojo class for outputting result to JDBC.
   */
  static class OutputPojo
  {
    private Double avgSpeed;
    private Boolean slowdownEvent;
    private String key;
    private Long timestamp;

    public OutputPojo()
    {
    }

    public OutputPojo(Double avgSpeed, Boolean slowdownEvent, String key, Long timestamp)
    {
      this.avgSpeed = avgSpeed;
      this.slowdownEvent = slowdownEvent;
      this.key = key;
      this.timestamp = timestamp;
    }

    @Override
    public String toString()
    {
      return key + " + " + avgSpeed + " + " + slowdownEvent + " + " + timestamp;
    }

    public void setTimestamp(Long timestamp)
    {
      this.timestamp = timestamp;
    }

    public Long getTimestamp()
    {
      return timestamp;
    }

    public void setAvgSpeed(Double avgSpeed)
    {
      this.avgSpeed = avgSpeed;
    }

    public Double getAvgSpeed()
    {
      return avgSpeed;
    }

    public void setKey(String key)
    {
      this.key = key;
    }

    public String getKey()
    {
      return key;
    }

    public void setSlowdownEvent(Boolean slowdownEvent)
    {
      this.slowdownEvent = slowdownEvent;
    }

    public Boolean getSlowdownEvent()
    {
      return slowdownEvent;
    }

  }

  public static class Collector extends BaseOperator
  {
    private static Map<KeyValPair<Long, String>, KeyValPair<Double, Boolean>> result = new HashMap<>();

    public static Map<KeyValPair<Long, String>, KeyValPair<Double, Boolean>> getResult()
    {
      return result;
    }

    public final transient DefaultInputPort<OutputPojo> input = new DefaultInputPort<OutputPojo>()
    {
      @Override
      public void process(OutputPojo tuple)
      {
        result.put(new KeyValPair<Long, String>(tuple.getTimestamp(), tuple.getKey()), new KeyValPair<Double, Boolean>(tuple.getAvgSpeed(), tuple.getSlowdownEvent()));
      }
    };
  }

  /**
   * Format the results of the slowdown calculations to a OutputPojo.
   */
  static class FormatStatsFn implements Function.MapFunction<Tuple.TimestampedTuple<KeyValPair<String, RouteInfo>>, OutputPojo>
  {
    @Override
    public OutputPojo f(Tuple.TimestampedTuple<KeyValPair<String, RouteInfo>> input)
    {
      RouteInfo routeInfo = input.getValue().getValue();
      OutputPojo row = new OutputPojo(routeInfo.getAvgSpeed(), routeInfo.getSlowdownEvent(), input.getValue().getKey(), input.getTimestamp());
      return row;
    }
  }


  /**
   * This composite transformation extracts speed info from traffic station readings.
   * It groups the readings by 'route' and analyzes traffic slowdown for that route.
   * Lastly, it formats the results for JDBC.
   */
  static class TrackSpeed extends
      CompositeStreamTransform<WindowedStream<KeyValPair<String, StationSpeed>>, WindowedStream<OutputPojo>>
  {
    @Override
    public WindowedStream<OutputPojo> compose(WindowedStream<KeyValPair<String, StationSpeed>> inputStream)
    {
      // Apply a GroupByKey transform to collect a list of all station
      // readings for a given route.
      WindowedStream<Tuple.WindowedTuple<KeyValPair<String, List<StationSpeed>>>> timeGroup =
          inputStream
          .accumulateByKey(new Group<StationSpeed>(), new Function.ToKeyValue<KeyValPair<String, StationSpeed>, String, StationSpeed>()
          {
            @Override
            public Tuple<KeyValPair<String, StationSpeed>> f(KeyValPair<String, StationSpeed> input)
            {
              return new Tuple.TimestampedTuple<>(input.getValue().getTimestamp(), input);
            }
          }, name("GroupByKey"));

      // Analyze 'slowdown' over the route readings.
      WindowedStream<Tuple.TimestampedTuple<KeyValPair<String, RouteInfo>>> stats = timeGroup
          .flatMap(new GatherStats(), name("GatherStats"));

      // Format the results for writing to JDBC table.
      WindowedStream<OutputPojo> results = stats.map(new FormatStatsFn(), name("FormatStatsFn"));

      return results;
    }
  }


  private static Double tryParseAvgSpeed(String[] inputItems)
  {
    try {
      return Double.parseDouble(tryParseString(inputItems, 3));
    } catch (NumberFormatException e) {
      return null;
    } catch (NullPointerException e) {
      return null;
    }
  }

  private static String tryParseStationType(String[] inputItems)
  {
    return tryParseString(inputItems, 2);
  }

  private static String tryParseStationId(String[] inputItems)
  {
    return tryParseString(inputItems, 1);
  }

  private static String tryParseTimestamp(String[] inputItems)
  {
    return tryParseString(inputItems, 0);
  }

  private static String tryParseString(String[] inputItems, int index)
  {
    return inputItems.length >= index ? inputItems[index] : null;
  }

  /**
   * Define some small hard-wired San Diego 'routes' to track based on sensor station ID.
   */
  private static Map<String, String> buildStationInfo()
  {
    Map<String, String> stations = new Hashtable<String, String>();
    stations.put("1108413", "SDRoute1"); // from freeway 805 S
    stations.put("1108699", "SDRoute2"); // from freeway 78 E
    stations.put("1108702", "SDRoute2");
    return stations;
  }

  /**
   * A dummy generator to generate some traffic information.
   */
  public static class InfoGen extends BaseOperator implements InputOperator
  {
    public transient DefaultOutputPort<String> output = new DefaultOutputPort<>();

    private String[] stationTypes = new String[]{"ML", "BL", "GL"};
    private int[] stationIDs = new int[]{1108413, 1108699, 1108702};
    private double ave = 55.0;
    private long timestamp;
    private static final Duration RAND_RANGE = Duration.standardMinutes(10);
    private static int tupleCount = 0;

    public static int getTupleCount()
    {
      return tupleCount;
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
      tupleCount = 0;
      timestamp = System.currentTimeMillis();
    }

    @Override
    public void emitTuples()
    {
      for (String stationType : stationTypes) {
        for (int stationID : stationIDs) {
          double speed = Math.random() * 20 + ave;
          long time = (long)(Math.random() * RAND_RANGE.getMillis()) + timestamp;
          try {
            output.emit(time + "," + stationID + "," + stationType + "," + speed);
            tupleCount++;

            Thread.sleep(50);
          } catch (Exception e) {
            // Ignore it
          }
        }
      }
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    InfoGen infoGen = new InfoGen();
    Collector collector = new Collector();

    // Create a stream from the input operator.
    ApexStream<Tuple.TimestampedTuple<String>> stream = StreamFactory.fromInput(infoGen, infoGen.output, name("infoGen"))

        // Extract the timestamp from the input and wrap it into a TimestampedTuple.
        .map(new ExtractTimestamps(), name("ExtractTimestamps"));

    stream
        // Extract the average speed of a station.
        .flatMap(new ExtractStationSpeedFn(), name("ExtractStationSpeedFn"))

        // Apply window and trigger option.
        .window(new WindowOption.SlidingTimeWindows(Duration.standardMinutes(WINDOW_DURATION), Duration.standardMinutes(WINDOW_SLIDE_EVERY)), new TriggerOption().withEarlyFiringsAtEvery(Duration.millis(5000)).accumulatingFiredPanes())

        // Apply TrackSpeed composite transformation to compute the route information.
        .addCompositeStreams(new TrackSpeed())

        // print the result to console.
        .print(name("console"))
        .endWith(collector, collector.input, name("Collector"))
        .populateDag(dag);
  }
}
