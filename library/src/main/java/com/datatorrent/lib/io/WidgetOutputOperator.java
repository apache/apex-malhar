/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io;

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.common.util.PubSubMessageCodec;
import com.datatorrent.common.util.PubSubWebSocketClient;

/**
 * This operator outputs data in a format that can be displayed in DT UI widgets.
 * <p>
 * There are 4 input ports each of which is compatible to one widget
 *  <li>simpleInput is used for simple input widget. It takes any object and push the toString() value to the UI</li>
 *  <li>timeSeriesInput is used for a widget of bar chart of series number values at certain times. It takes a Long for time and a Number for value </li>
 *  <li>percentageInput is used for either the percentage gadget or progress bar. It takes int value between 0 and 100 as input</li>
 *  <li>topNInput is used for N key value table widget. It takes a Map as input</li><br>
 *
 *  By default it outputs data to WebSocket channel specified by DT gateway.<br>
 *  If DT gateway is not specified, it will use output data to console.
 * </p>
 * @displayName DT Widget Output
 * @category Output
 * @tags dt, output operator
 *
 * @since 0.9.3
 */
public class WidgetOutputOperator extends BaseOperator
{
  protected transient WebSocketOutputOperator<Pair<String, Object>> wsoo = new WebSocketOutputOperator<Pair<String,Object>>(){

    private transient PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);

    @Override
    public String convertMapToMessage(Pair<String,Object> t) throws IOException {
      return PubSubWebSocketClient.constructPublishMessage(t.getLeft(), t.getRight(), codec);
    };

  };

  protected transient ConsoleOutputOperator coo = new ConsoleOutputOperator();

  private String timeSeriesTopic = "widget.timeseries";

  private String simpleTopic = "widget.simple";

  private String percentageTopic = "widget.percentage";

  protected String topNTopic = "widget.topn";

  private String pieChartTopic = "widget,piechart";

  private Number timeSeriesMax = 100;

  private Number timeSeriesMin = 0;

  protected int nInTopN = 10;

  private int nInPie = 5;

  private transient String appId = null;

  private transient int operId = 0;

  /**
   * Tuples received on this input port will be sent to a Simple Widget for display.
   */
  @InputPortFieldAnnotation(optional=true)
  public final transient SimpleInputPort simpleInput = new SimpleInputPort(this);

  /**
   * Tuples received on this input port will be sent to a Time Series Widget for display.
   */
  @InputPortFieldAnnotation(optional=true)
  public final transient TimeseriesInputPort timeSeriesInput = new TimeseriesInputPort(this);

  /**
   * Tuples received on this input port will be sent to a Percentage Widget.
   */
  @InputPortFieldAnnotation(optional=true)
  public final transient PercentageInputPort percentageInput = new PercentageInputPort(this);

  /**
   * Tuples received on this input port will be sent to a Top N Widget for display.
   */
  @InputPortFieldAnnotation(optional=true)
  public final transient TopNInputPort topNInput = new TopNInputPort(this);

  /**
   * Tuples received on this input port will be sent to a Pie Chart Widget for display.
   */
  @InputPortFieldAnnotation(optional=true)
  public final transient PiechartInputPort pieChartInput = new PiechartInputPort(this);

  protected transient boolean isWebSocketConnected = true;

  @Override
  public void setup(OperatorContext context)
  {
    String gatewayAddress = context.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    if(!StringUtils.isEmpty(gatewayAddress)){
      wsoo.setUri(URI.create("ws://" + gatewayAddress + "/pubsub"));
      wsoo.setup(context);
    } else {
      isWebSocketConnected = false;
      coo.setup(context);
    }
    appId = context.getValue(DAG.APPLICATION_ID);
    operId = context.getId();

  }

  public static class TimeSeriesData{

    public Long time;

    public Number data;

  }

  public static class TimeseriesInputPort extends DefaultInputPort<TimeSeriesData[]> {

    private final WidgetOutputOperator operator;

    public TimeseriesInputPort(WidgetOutputOperator woo)
    {
      operator = woo;
    }

    @Override
    public void process(TimeSeriesData[] tuple)
    {
      @SuppressWarnings({"unchecked", "rawtypes"})
      HashMap<String, Number>[] timeseriesMapData = new HashMap[tuple.length];
      int i = 0;
      for (TimeSeriesData data : tuple) {
        HashMap<String, Number> timeseriesMap = Maps.newHashMapWithExpectedSize(2);
        timeseriesMap.put("timestamp", data.time);
        timeseriesMap.put("value", data.data);
        timeseriesMapData[i++] = timeseriesMap;
      }

      if(operator.isWebSocketConnected){
        HashMap<String, Object> schemaObj = new HashMap<String, Object>();
        schemaObj.put("type", "timeseries");
        schemaObj.put("minValue", operator.timeSeriesMin);
        schemaObj.put("maxValue", operator.timeSeriesMax);
        operator.wsoo.input.process(new MutablePair<String, Object>(operator.getFullTopic( operator.timeSeriesTopic, schemaObj), timeseriesMapData));
      } else {
        operator.coo.input.process(tuple);
      }
    }

    public TimeseriesInputPort setMax(Number max){
      operator.timeSeriesMax = max;
      return this;
    }


    public TimeseriesInputPort setMin(Number min){
      operator.timeSeriesMin = min;
      return this;
    }

    public TimeseriesInputPort setTopic(String topic){
      operator.timeSeriesTopic = topic;
      return this;
    }

  }

  public static class TopNInputPort extends DefaultInputPort<HashMap<String, Number>>{

    private final WidgetOutputOperator operator;

    public TopNInputPort(WidgetOutputOperator oper)
    {
      operator = oper;
    }

    @Override
    public void process(HashMap<String, Number> topNMap)
    {
      @SuppressWarnings({"unchecked", "rawtypes"})
      HashMap<String, Object>[] result = new HashMap[topNMap.size()];
      int j = 0;
      for (Entry<String, Number> e : topNMap.entrySet()) {
        result[j] = new HashMap<String, Object>();
        result[j].put("name", e.getKey());
        result[j++].put("value", e.getValue());
      }
      if(operator.isWebSocketConnected){
        HashMap<String, Object> schemaObj = new HashMap<String, Object>();
        schemaObj.put("type", "topN");
        schemaObj.put("n", operator.nInTopN);
        operator.wsoo.input.process(new MutablePair<String, Object>(operator.getFullTopic(operator.topNTopic, schemaObj), result));
      } else {
        operator.coo.input.process(topNMap);
      }
    }

    public TopNInputPort setN(int n){
      operator.nInTopN = n;
      return this;
    }

    public TopNInputPort setTopic(String topic)
    {
      operator.topNTopic = topic;
      return this;
    }

  }

  public static class SimpleInputPort extends DefaultInputPort<Object>{

    private final WidgetOutputOperator operator;

    public SimpleInputPort(WidgetOutputOperator oper)
    {
      operator = oper;
    }

    @Override
    public void process(Object tuple)
    {

      if (operator.isWebSocketConnected) {
        HashMap<String, Object> schemaObj = new HashMap<String, Object>();
        schemaObj.put("type", "simple");
        operator.wsoo.input.process(new MutablePair<String, Object>(operator.getFullTopic(operator.simpleTopic, schemaObj), tuple.toString()));
      } else {
        operator.coo.input.process(tuple);
      }
    }

    public SimpleInputPort setTopic(String topic) {
      operator.simpleTopic = topic;
      return this;
    }
  }

  public static class PercentageInputPort extends DefaultInputPort<Integer>
  {
    private final WidgetOutputOperator operator;

    public PercentageInputPort(WidgetOutputOperator oper)
    {
      operator = oper;
    }

    @Override
    public void process(Integer tuple)
    {
      if(operator.isWebSocketConnected){
        HashMap<String, Object> schemaObj = new HashMap<String, Object>();
        schemaObj.put("type", "percentage");
        operator.wsoo.input.process(new MutablePair<String, Object>(operator.getFullTopic(operator.percentageTopic, schemaObj), tuple));
      } else {
        operator.coo.input.process(tuple);
      }
    }

    public PercentageInputPort setTopic(String topic)
    {
      operator.percentageTopic = topic;
      return this;
    }
  }

public static class PiechartInputPort extends DefaultInputPort<HashMap<String, Number>>{

    private final WidgetOutputOperator operator;

    public PiechartInputPort(WidgetOutputOperator oper)
    {
      operator = oper;
    }

    @Override
    public void process(HashMap<String, Number> pieNumbers)
    {
      @SuppressWarnings("unchecked")
      HashMap<String, Object>[] result = (HashMap<String, Object>[])Array.newInstance(HashMap.class, pieNumbers.size());

      int j = 0;
      for (Entry<String, Number> e : pieNumbers.entrySet()) {
        result[j] = new HashMap<String, Object>();
        result[j].put("label", e.getKey());
        result[j++].put("value", e.getValue());
      }
      if(operator.isWebSocketConnected){
        HashMap<String, Object> schemaObj = new HashMap<String, Object>();
        schemaObj.put("type", "piechart");
        schemaObj.put("n", operator.nInPie);
        operator.wsoo.input.process(new MutablePair<String, Object>(operator.getFullTopic(operator.pieChartTopic, schemaObj), result));
      } else {
        operator.coo.input.process(pieNumbers);
      }
    }

    public PiechartInputPort setN(int n){
      operator.nInPie = n;
      return this;
    }

    public PiechartInputPort setTopic(String topic)
    {
      operator.pieChartTopic = topic;
      return this;
    }

  }

  protected String getFullTopic(String topic, Map<String, Object> schema){
    HashMap<String, Object> topicObj = new HashMap<String, Object>();
    topicObj.put("appId", appId);
    topicObj.put("opId", operId);
    topicObj.put("topicName", topic);
    topicObj.put("schema", schema);
    try {
      return "AppData" + wsoo.mapper.writeValueAsString(topicObj);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown()
  {
    if(isWebSocketConnected){
      wsoo.teardown();
    } else {
      coo.teardown();
    }
  }

}
