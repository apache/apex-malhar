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

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;


import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.appdata.AbstractAppDataServer;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.experimental.AppData;


/**
 * Operator that reads the KeyValPair tuples from the Windowed Operator and serves live queries.
 *
 * The KeyValPair input tuples are zip to total payment of the window. They are collected by an internal map so that
 * the data can be served.
 *
 * @since 3.8.0
 */
public class NycTaxiDataServer extends AbstractAppDataServer<String>
{
  public final transient DefaultInputPort<Tuple.WindowedTuple<KeyValPair<String, Double>>> input = new DefaultInputPort<Tuple.WindowedTuple<KeyValPair<String, Double>>>()
  {
    @Override
    public void process(Tuple.WindowedTuple<KeyValPair<String, Double>> tuple)
    {
      if (!currentWindowHasData) {
        currentData = new HashMap<>();
        currentWindowHasData = true;
      }
      KeyValPair<String, Double> tupleValue = tuple.getValue();
      currentData.put(tupleValue.getKey(), tupleValue.getValue());
    }
  };

  @AppData.ResultPort
  public final transient DefaultOutputPort<String> queryResult = new DefaultOutputPort<>();

  private Map<String, Double> servingData = new HashMap<>();
  private transient Map<String, Double> currentData = new HashMap<>();
  private transient ArrayDeque<String> resultQueue = new ArrayDeque<>();
  private boolean currentWindowHasData = false;

  @Override
  public void beginWindow(long l)
  {
    super.beginWindow(l);
    currentWindowHasData = false;
  }

  @Override
  public void endWindow()
  {
    while (!resultQueue.isEmpty()) {
      String result = resultQueue.remove();
      queryResult.emit(result);
    }
    servingData = currentData;
    super.endWindow();
  }


  @Override
  protected void processQuery(String queryStr)
  {
    try {
      JSONObject query = new JSONObject(queryStr);
      JSONObject result = new JSONObject();
      double lat = query.getDouble("lat");
      double lon = query.getDouble("lon");
      Pair<String, String> zips = recommendZip(lat, lon);
      result.put("currentZip", zips.getLeft());
      result.put("driveToZip", zips.getRight());
      resultQueue.add(result.toString());
    } catch (JSONException e) {
      LOG.error("Unrecognized query: {}", queryStr);
    }
  }

  public Pair<String, String> recommendZip(double lat, double lon)
  {
    String currentZip = NycLocationUtils.getZip(lat, lon);
    String zip = currentZip;
    String[] neighboringZips = NycLocationUtils.getNeighboringZips(zip);
    double dollars = servingData.containsKey(zip) ? servingData.get(zip) : 0;
    LOG.info("Current zip: {}={}", zip, dollars);
    for (String neigboringZip : neighboringZips) {
      double tmpDollars = servingData.containsKey(neigboringZip) ? servingData.get(neigboringZip) : 0;
      LOG.info("Neighboring zip: {}={}", neigboringZip, tmpDollars);
      if (tmpDollars > dollars) {
        dollars = tmpDollars;
        zip = neigboringZip;
      }
    }
    return new ImmutablePair<>(currentZip, zip);
  }

  private static final Logger LOG = LoggerFactory.getLogger(NycTaxiDataServer.class);

}
