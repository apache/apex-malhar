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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Operator that parses historical New York City Yellow Cab ride data
 * from http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml.
 *
 * @since 3.8.0
 */
public class NycTaxiCsvParser extends BaseOperator
{
  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      String[] values = tuple.split(",", -1);
      Map<String, String> outputTuple = new HashMap<>();
      if (values.length > 18 && StringUtils.isNumeric(values[0])) {
        outputTuple.put("pickup_time", values[1]);
        outputTuple.put("pickup_lon", values[5]);
        outputTuple.put("pickup_lat", values[6]);
        outputTuple.put("total_fare", values[18]);
        output.emit(outputTuple);
      } else {
        LOG.warn("Dropping tuple with unrecognized format: {}", tuple);
      }
    }
  };

  public final transient DefaultOutputPort<Map<String, String>> output = new DefaultOutputPort<>();
  private static final Logger LOG = LoggerFactory.getLogger(NycTaxiCsvParser.class);
}
