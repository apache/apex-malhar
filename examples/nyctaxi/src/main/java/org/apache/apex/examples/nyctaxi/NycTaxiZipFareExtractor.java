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
package org.apache.apex.examples.nyctaxi;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.ControlTuple;
import org.apache.apex.malhar.lib.window.Tuple;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Operator that fills in the zip code based on the lat-lon coordinates in the incoming tuples and prepares
 * the tuples for the WindowedOperator downstream. It also generates a watermark that is t - 1 minute.
 *
 * @since 3.8.0
 */
public class NycTaxiZipFareExtractor extends BaseOperator
{
  public static class Watermark implements ControlTuple.Watermark
  {
    private long timestamp;

    private Watermark()
    {
      // for kryo
    }

    public Watermark(long timestamp)
    {
      this.timestamp = timestamp;
    }

    @Override
    public long getTimestamp()
    {
      return this.timestamp;
    }
  }

  public final transient DefaultInputPort<Map<String, String>> input = new DefaultInputPort<Map<String, String>>()
  {
    @Override
    public void process(Map<String, String> tuple)
    {
      try {
        String zip = NycLocationUtils.getZip(Double.valueOf(tuple.get("pickup_lat")), Double.valueOf(tuple.get("pickup_lon")));
        Date date = dateFormat.parse(tuple.get("pickup_time"));
        long timestamp = date.getTime();
        double fare = Double.valueOf(tuple.get("total_fare"));
        output.emit(new Tuple.TimestampedTuple<>(timestamp, new KeyValPair<>(zip, fare)));
        if (timestamp > currentTimestamp) {
          currentTimestamp = timestamp;
          watermarkOutput.emit(new Watermark(timestamp - 60 * 1000));
        }
      } catch (ParseException ex) {
        LOG.warn("Ignoring tuple with bad timestamp {}", tuple.get("pickup_time"));
      }
    }
  };

  public final transient DefaultOutputPort<Tuple.TimestampedTuple<KeyValPair<String, Double>>> output = new DefaultOutputPort<>();
  public final transient DefaultOutputPort<Watermark> watermarkOutput = new DefaultOutputPort<>();

  private transient SimpleDateFormat dateFormat;
  private long currentTimestamp = -1;

  private static final Logger LOG = LoggerFactory.getLogger(NycTaxiZipFareExtractor.class);

  @Override
  public void setup(OperatorContext context)
  {
    dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    dateFormat.setTimeZone(TimeZone.getTimeZone("America/New_York"));
  }

}
