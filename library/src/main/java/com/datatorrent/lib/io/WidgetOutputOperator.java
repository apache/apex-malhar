package com.datatorrent.lib.io;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.util.PubSubMessageCodec;
import com.datatorrent.api.util.PubSubWebSocketClient;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.google.common.collect.Maps;

public class WidgetOutputOperator extends WebSocketOutputOperator<Pair<String, Object>>
{ 
  private transient PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);
  
  private ConsoleOutputOperator coo = new ConsoleOutputOperator();
  
  private String timeSeriesTopic = "widget.timeseries";
  
  private String simpleTopic = "widget.simple";
  
  private String percentageTopic = "widget.percentage";
  
  private String topNTopic = "widget.topn";
  
  private Number timeSeriesMax = 100;
  
  private Number timeSeriesMin = 0;
  
  private int nInTopN = 10;
  
  private transient String appId = null;
  
  private transient int operId = 0; 
  
  @InputPortFieldAnnotation(name="simple input", optional=true)
  public final transient SimpleInputPort simpleInput = new SimpleInputPort(this);
  
  @InputPortFieldAnnotation(name="time series input", optional=true)
  public final transient TimeseriesInputPort timeSeriesInput = new TimeseriesInputPort(this);

  @InputPortFieldAnnotation(name="percentage input", optional=true)
  public final transient PercentageInputPort percentageInput = new PercentageInputPort(this);
  
  @InputPortFieldAnnotation(name="topN input", optional=true)
  public final transient TopNInputPort topNInput = new TopNInputPort(this);
  
  private transient boolean isWebSocketConnected = true;
  
  @Override
  public void setup(OperatorContext context)
  {
    String gatewayAddress = context.getValue(DAG.GATEWAY_ADDRESS);
    if(!StringUtils.isEmpty(gatewayAddress)){
      setUri(URI.create("ws://" + gatewayAddress + "/pubsub"));
      super.setup(context);
    } else {
      isWebSocketConnected = false;
      coo.setup(context);
    }
    appId = context.getValue(DAG.APPLICATION_ID);
    operId = context.getId();
    
  }
  
  @Override
  public String convertMapToMessage(Pair<String, Object> t) throws IOException {
    return PubSubWebSocketClient.constructPublishMessage(t.getLeft(), t.getRight(), codec);
  };
  
  public static class TimeSeriesData{
    
    public Long time;
    
    public Number data;
    
  }
  
  public static class TimeseriesInputPort extends DefaultInputPort<TimeSeriesData[]> {

    private WidgetOutputOperator operator;
    
    public TimeseriesInputPort(WidgetOutputOperator woo)
    {
      operator = woo;
    }
    
    @Override
    public void process(TimeSeriesData[] tuple)
    {
      @SuppressWarnings("unchecked")
      HashMap<String, Number>[] timeseriesMapData = new HashMap[tuple.length];
      int i = 0;
      for (TimeSeriesData data : tuple) {
        HashMap<String, Number> timeseriesMap = Maps.newHashMapWithExpectedSize(2);
        timeseriesMap.put("timestamp", data.time);
        timeseriesMap.put("value", data.data);
        timeseriesMapData[i++] = timeseriesMap;
      }

      if(operator.isWebSocketConnected){
        operator.input.process(new MutablePair<String, Object>(operator.appId + "." + operator.operId +
            "." + operator.timeSeriesTopic + "_{\"type\":\"timeseries\",\"minValue\":" + operator.timeSeriesMin + 
            ",\"maxValue\":" + operator.timeSeriesMax + "}", timeseriesMapData));
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
    
    private WidgetOutputOperator operator;
    
    public TopNInputPort(WidgetOutputOperator oper)
    {
      operator = oper;
    }

    @Override
    public void process(HashMap<String, Number> topNMap)
    {
      @SuppressWarnings("unchecked")
      HashMap<String, Object>[] result = new HashMap[topNMap.size()];
      int j = 0;
      for (Entry<String, Number> e : topNMap.entrySet()) {
        result[j] = new HashMap<String, Object>();
        result[j].put("name", e.getKey());
        result[j++].put("value", e.getValue());
      }
      if(operator.isWebSocketConnected){
        operator.input.process(new MutablePair<String, Object>(operator.appId + "." + operator.operId +
            "." + operator.topNTopic + "_{\"type\":\"topN\",\"n\":" + operator.nInTopN + "}", result));
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
    
    private WidgetOutputOperator operator;
    
    public SimpleInputPort(WidgetOutputOperator oper)
    {
      operator = oper;
    }

    @Override
    public void process(Object tuple)
    {
      
      if (operator.isWebSocketConnected) {
        operator.input.process(new MutablePair<String, Object>(operator.appId + "." + operator.operId + "." + operator.simpleTopic + "_{\"type\":\"simple\"}", tuple.toString()));
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
    private WidgetOutputOperator operator;

    public PercentageInputPort(WidgetOutputOperator oper)
    {
      operator = oper;
    }

    @Override
    public void process(Integer tuple)
    {
      if(operator.isWebSocketConnected){
        operator.input.process(new MutablePair<String, Object>(operator.appId + "." + operator.operId +
            "." + operator.percentageTopic + "_{\"type\":\"percentage\"}", tuple));
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

}