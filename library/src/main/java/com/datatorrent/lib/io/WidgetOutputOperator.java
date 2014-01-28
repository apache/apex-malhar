package com.datatorrent.lib.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.util.PubSubMessageCodec;
import com.datatorrent.api.util.PubSubWebSocketClient;
import com.datatorrent.api.DefaultInputPort;
import com.google.common.collect.Maps;

public class WidgetOutputOperator extends WebSocketOutputOperator<Pair<String, Object>>
{ 
  private transient PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);
  
  private String timeSeriesPrefix = "widget.timeseries";
  
  private String simplePrefix = "widget.simple";
  
  private String percentagePrefix = "widget.percentage";
  
  private String topNPrefix = "widget.topn";
  
  private Number timeSeriesMax = 100;
  
  private Number timeSeriesMin = 0;
  
  private int nInTopN = 10;
  
  @InputPortFieldAnnotation(name="simple input", optional=true)
  public final transient SimpleInputPort simpleInput = new SimpleInputPort(this);
  
  @InputPortFieldAnnotation(name="time series input", optional=true)
  public final transient TimeseriesInputPort timeSeriesInput = new TimeseriesInputPort(this);

  @InputPortFieldAnnotation(name="percentage input", optional=true)
  public final transient PercentageInputPort percentageInput = new PercentageInputPort(this);
  
  @InputPortFieldAnnotation(name="topN input", optional=true)
  public final transient TopNInputPort topNInput = new TopNInputPort(this);
  
  @Override
  public String convertMapToMessage(Pair<String, Object> t) throws IOException {
    return PubSubWebSocketClient.constructPublishMessage(t.getLeft(), t.getRight(), codec);
  };
  
  public static class TimeSeriesData{
    
    public Long time;
    
    public Number data;
    
  }
  
  public static class TimeseriesInputPort extends DefaultInputPort<TimeSeriesData> {

    private WidgetOutputOperator operator;
    
    public TimeseriesInputPort(WidgetOutputOperator woo)
    {
      operator = woo;
    }
    
    @Override
    public void process(TimeSeriesData tuple)
    {
      HashMap<String, Number> timeseriesMap = Maps.newHashMapWithExpectedSize(2);
      timeseriesMap.put("timestamp", tuple.time);
      timeseriesMap.put("value", tuple.data);
      operator.input.process(new MutablePair<String, Object>(operator.timeSeriesPrefix + "_{\"type\":\"timeseries\",\"minValue\":" + operator.timeSeriesMin + 
          ",\"maxValue\":" + operator.timeSeriesMax + "}", timeseriesMap));
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
      operator.timeSeriesPrefix = topic;
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
      operator.input.process(new MutablePair<String, Object>(operator.topNPrefix + "_{\"type\":\"topN\",\"n\":" + operator.nInTopN + "}", result));
    }
    
    public TopNInputPort setN(int n){
      operator.nInTopN = n;
      return this;
    }
    
    public TopNInputPort setTopic(String topic)
    {
      operator.topNPrefix = topic;
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
      operator.input.process(new MutablePair<String, Object>(operator.simplePrefix + "_{\"type\":\"simple\"}", tuple.toString()));
    }
    
    public SimpleInputPort setTopic(String topic) {
      operator.simplePrefix = topic;
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
      operator.input.process(new MutablePair<String, Object>(operator.percentagePrefix + "_{\"type\":\"percentage\"}", tuple));
    }

    public PercentageInputPort setTopic(String topic)
    {
      operator.percentagePrefix = topic;
      return this;
    }
  }

}