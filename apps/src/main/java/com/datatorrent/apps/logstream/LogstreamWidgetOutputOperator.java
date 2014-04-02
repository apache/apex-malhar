/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.apps.logstream;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.tuple.MutablePair;

import com.datatorrent.lib.io.WidgetOutputOperator;
import com.datatorrent.lib.logs.DimensionObject;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;

import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;
import com.datatorrent.apps.logstream.PropertyRegistry.PropertyRegistry;
import java.lang.reflect.Array;

/**
 * Output operator to send outputs to websocket to display on UI widgets.
 *
 */
public class LogstreamWidgetOutputOperator extends WidgetOutputOperator
{
  @NotNull
  private PropertyRegistry<String> registry;
  public final transient LogstreamTopNInputPort logstreamTopNInput = new LogstreamTopNInputPort(LogstreamWidgetOutputOperator.this);

  /**
   * supply the registry object which is used to store and retrieve meta information about each tuple
   *
   * @param registry
   */
  public void setRegistry(PropertyRegistry<String> registry)
  {
    this.registry = registry;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    LogstreamPropertyRegistry.setInstance(registry);
  }

  /**
   * Creates the widget output object from input tuple, populates all the meta information and sends to websocket
   */
  public class LogstreamTopNInputPort extends DefaultInputPort<HashMap<String, ArrayList<DimensionObject<String>>>>
  {
    NumberFormat formatter = new DecimalFormat("#0.00");
    private LogstreamWidgetOutputOperator operator;

    public LogstreamTopNInputPort(LogstreamWidgetOutputOperator oper)
    {
      operator = oper;
    }

    @Override
    public void process(HashMap<String, ArrayList<DimensionObject<String>>> tuple)
    {
      for (Entry<String, ArrayList<DimensionObject<String>>> entry : tuple.entrySet()) {
        String keyString = entry.getKey();

        ArrayList<DimensionObject<String>> arrayList = entry.getValue();

        HashMap<String, Object> schemaObj = new HashMap<String, Object>();

        String[] keyInfo = keyString.split("\\|");
        HashMap<String, String> tupleMeta = new HashMap<String, String>();
        tupleMeta.put("timeBucket", keyInfo[0]);
        //appMeta.put("timeStamp", key[1]);
        tupleMeta.put("logType", registry.lookupValue(new Integer(keyInfo[2])));
        tupleMeta.put("filter", registry.lookupValue(new Integer(keyInfo[3])));
        tupleMeta.put("dimension", registry.lookupValue(new Integer(keyInfo[4])));
        String[] val = keyInfo[5].split("\\.");
        tupleMeta.put("value", val[0]);
        tupleMeta.put("metric", val[1]);

        schemaObj.put("tupleMeta", tupleMeta);

        String keyTitle = tupleMeta.get("dimension");
        String valueTitle = tupleMeta.get("metric") + "(" + tupleMeta.get("value") + ")";

        schemaObj.put("keyTitle", keyTitle);
        schemaObj.put("valueTitle", valueTitle);

        String topic = keyInfo[0] + "|" + keyInfo[2] + "|" + keyInfo[3] + "|" + keyInfo[4] + "|" + keyInfo[5];

        setTopic(topic);

        HashMap<String, Number> topNMap = new HashMap<String, Number>();

        for (DimensionObject<String> dimensionObject : arrayList) {
          topNMap.put(dimensionObject.getVal(), dimensionObject.getCount());
        }

        this.processTopN(topNMap, schemaObj);

      }

    }

    private void processTopN(HashMap<String, Number> topNMap, HashMap<String, Object> schemaObj)
    {
      @SuppressWarnings("unchecked")
      HashMap<String, Object>[] result = (HashMap<String, Object>[])Array.newInstance(HashMap.class, topNMap.size());
      
      int j = 0;
      for (Entry<String, Number> e : topNMap.entrySet()) {
        result[j] = new HashMap<String, Object>();
        result[j].put("name", e.getKey());
        String val = formatter.format(e.getValue());
        result[j++].put("value", val);
      }
      if (operator.isWebSocketConnected) {
        schemaObj.put("type", "topN");
        schemaObj.put("n", operator.nInTopN);
        operator.wsoo.input.process(new MutablePair<String, Object>(operator.getFullTopic(operator.topNTopic, schemaObj), result));
      }
      else {
        operator.coo.input.process(topNMap);
      }

    }

    public LogstreamTopNInputPort setN(int n)
    {
      operator.nInTopN = n;
      return this;
    }

    public LogstreamTopNInputPort setTopic(String topic)
    {
      operator.topNTopic = topic;
      return this;
    }

  }

}
