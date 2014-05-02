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
import com.datatorrent.apps.etl.Chart.LineChartParams;

/**
 *
 */
public class RealTimeOutputOperator extends BaseOperator
{
  protected transient String appId = null;
  protected transient int operId = 0;
  protected transient boolean isWebSocketConnected;
  protected HashSet<Integer> publishedSchemas;
  @Nonnull
  protected List<LineChartParams> lineChartParamsList;
  private boolean isTest = true;
  public final transient DefaultInputPort<Map<Object, Object>> lineChartInput = new DefaultInputPort<Map<Object, Object>>()
  {
    @Override
    public void process(Map<Object, Object> t)
    {
      processTuple(t);
    }

  };

  private void processTuple(Map<Object, Object> t)
  {
    for (Entry<Object, Object> entry : t.entrySet()) {
      Integer schemaId = (Integer)entry.getKey();
      Object data = entry.getValue();
      if (isWebSocketConnected) {
        if (isTest) {
          HashMap<Object, Object> out = new HashMap<Object, Object>();
          out.put("meta", lineChartParamsList.get(schemaId).getMeta());
          out.put("data", data);
          wsoo.input.process(new MutablePair<String, Object>(generateTopic(schemaId), out));
        }
        else {
          if (!publishedSchemas.contains(schemaId)) {
            wsoo.input.process(new MutablePair<String, Object>(generateTopic(schemaId), lineChartParamsList.get(schemaId).getMeta()));
            publishedSchemas.add(schemaId);
          }
          wsoo.input.process(new MutablePair<String, Object>(generateTopic(schemaId), data));
        }
      }
    }
  }

  private String generateTopic(Object schemaId)
  {
    return new StringBuilder().append(appId).append(".").append(operId).append(".").append(schemaId).toString();
  }

  protected transient WebSocketOutputOperator<Pair<String, Object>> wsoo = new WebSocketOutputOperator<Pair<String, Object>>()
  {
    private transient PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);

    @Override
    public String convertMapToMessage(Pair<String, Object> t) throws IOException
    {
      String msg = PubSubWebSocketClient.constructPublishMessage(t.getLeft(), t.getRight(), codec);
      System.out.println("#ashwin converted msg" + msg);
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
    else {
      isWebSocketConnected = false;
    }
    appId = context.getValue(DAG.APPLICATION_ID);
    operId = context.getId();
    publishedSchemas = Sets.newHashSet();
  }

  public List<LineChartParams> getChartParamsList()
  {
    return lineChartParamsList;
  }

  public void setChartParamsList(LineChartParams[] params)
  {
    this.lineChartParamsList = Lists.newArrayList(params);
  }

}
