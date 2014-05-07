/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.apps.etl;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.datatorrent.lib.io.WebSocketOutputOperator;
import com.datatorrent.lib.util.PubSubMessageCodec;
import com.datatorrent.lib.util.PubSubWebSocketClient;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.apps.etl.Chart.CHART_TYPE;
import com.datatorrent.apps.etl.Chart.LineChartParams;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real time chart output operator which accepts input from various {@link ChartOperator} implementations
 * and publishes the tuples to the gateway through websocket.
 *
 * For every new schema, the schema is sent first followed by the tuples corresponding to the schema.
 */
public class RealTimeChartOutputOperator extends BaseOperator
{
  private static final long serialVersionUID = 1L;
  protected transient String appId = null;
  protected transient int operId = 0;
  protected transient boolean isWebSocketConnected;
  protected transient Map<CHART_TYPE, Set<Integer>> publishedSchemas;
  protected Map<CHART_TYPE, List<Chart>> charts = new HashMap<CHART_TYPE, List<Chart>>();

  @InputPortFieldAnnotation(name = "lineChartInput", optional = true)
  public final transient DefaultInputPort<Map<Object, Object>> lineChartInput = new DefaultInputPort<Map<Object, Object>>()
  {
    @Override
    public void process(Map<Object, Object> t)
    {
      processTuple(t, CHART_TYPE.LINE);
    }

  };

  @InputPortFieldAnnotation(name = "topChartInput", optional = true)
  public final transient DefaultInputPort<Map<Object, Object>> topChartInput = new DefaultInputPort<Map<Object, Object>>()
  {
    @Override
    public void process(Map<Object, Object> t)
    {
      processTuple(t, CHART_TYPE.TOP);
    }
  };

  @InputPortFieldAnnotation(name = "mapChartInput", optional = true)
  public final transient DefaultInputPort<Map<Object, Object>> mapChartInput = new DefaultInputPort<Map<Object, Object>>()
  {
    @Override
    public void process(Map<Object, Object> t)
    {
      processTuple(t, CHART_TYPE.MAP);
    }
  };

  /**
   * generic process methods used to publish data for any input
   * @param t tuple
   * @param type chart type
   */
  private void processTuple(Map<Object, Object> t, CHART_TYPE type)
  {
    List<Chart> lineChartParamsList = charts.get(type);
    for (Entry<Object, Object> entry : t.entrySet()) {
      Integer schemaId = (Integer)entry.getKey();
      Object data = entry.getValue();
      if (isWebSocketConnected) {
        Set<Integer> schemas = publishedSchemas.get(type);
        if (schemas == null || !schemas.contains(schemaId)) {
          // publish schema
          wsoo.input.process(new MutablePair<String, Object>(generateTopic(type, schemaId), lineChartParamsList.get(schemaId).getMeta()));
          if (schemas == null) {
            schemas = new HashSet<Integer>();
            publishedSchemas.put(type, schemas);
          }
          schemas.add(schemaId);
        }
        // publish data
        wsoo.input.process(new MutablePair<String, Object>(generateTopic(type, schemaId), data));

      }
    }
  }

  private String generateTopic(CHART_TYPE type, Object schemaId)
  {
    return new StringBuilder().append(appId).append(".").append(operId).append(".").append(type).append(".").append(schemaId).toString();
  }

  protected transient WebSocketOutputOperator<Pair<String, Object>> wsoo = new WebSocketOutputOperator<Pair<String, Object>>()
  {
    private transient PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);

    @Override
    public String convertMapToMessage(Pair<String, Object> t) throws IOException
    {
      String msg = PubSubWebSocketClient.constructPublishMessage(t.getLeft(), t.getRight(), codec);
      logger.debug("converted message = {}", msg);
      return msg;
    }

  };

  @Override
  public void setup(OperatorContext context)
  {
    String gatewayAddress = context.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    if (!StringUtils.isEmpty(gatewayAddress)) {
      wsoo.setUri(URI.create("ws://" + gatewayAddress + "/pubsub"));
      wsoo.setup(context);
      isWebSocketConnected = true;
    }
    appId = context.getValue(DAG.APPLICATION_ID);
    operId = context.getId();
    publishedSchemas = new HashMap<CHART_TYPE, Set<Integer>>();
  }

  public void setLineChartParamsList(Chart[] chartParams)
  {
    ArrayList<Chart> params = Lists.newArrayList(chartParams);
    charts.put(CHART_TYPE.LINE, params);
  }

  public Chart[] getLineChartParamsList()
  {
    List<Chart> params = charts.get(CHART_TYPE.LINE);
    if (params == null || params.isEmpty()) {
      return null;
    }
    Chart[] paramArr = new Chart[params.size()];
    params.toArray(paramArr);
    return paramArr;
  }

  public void setTopChartParamsList(Chart[] chartParams)
  {
    ArrayList<Chart> params = Lists.newArrayList(chartParams);
    charts.put(CHART_TYPE.TOP, params);
  }

  public Chart[] getMapChartParamsList()
  {
    List<Chart> params = charts.get(CHART_TYPE.MAP);
    if (params == null || params.isEmpty()) {
      return null;
    }
    Chart[] paramArr = new Chart[params.size()];
    params.toArray(paramArr);
    return paramArr;
  }

  public void setMapChartParamsList(Chart[] chartParams)
  {
    ArrayList<Chart> params = Lists.newArrayList(chartParams);
    charts.put(CHART_TYPE.MAP, params);
  }

  public Chart[] getTopChartParamsList()
  {
    List<Chart> params = charts.get(CHART_TYPE.TOP);
    if (params == null || params.isEmpty()) {
      return null;
    }
    Chart[] paramArr = new Chart[params.size()];
    params.toArray(paramArr);
    return paramArr;
  }

  private static final Logger logger = LoggerFactory.getLogger(RealTimeChartOutputOperator.class);
}
