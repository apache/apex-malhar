package com.datatorrent.lib.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.util.PubSubMessageCodec;
import com.datatorrent.api.util.PubSubWebSocketClient;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.Pair;
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
  
  
  public void setup(OperatorContext context) {
  };
  
  public transient SimpleInputPort simpleInput = new SimpleInputPort(this);
  
  public transient TimeseriesInputPort timeSeriesInput = new TimeseriesInputPort(this);

  public transient PercentageInputPort percentageInput = new PercentageInputPort(this);
  
  public transient TopNInputPort topNInput = new TopNInputPort(this);
  
  @Override
  public String convertMapToMessage(Pair<String, Object> t) throws IOException {
    return PubSubWebSocketClient.constructPublishMessage(t.getFirst(), t.second, codec);
  };
  
  public static class TimeseriesInputPort extends DefaultInputPort<Pair<Long, Number>> {

    private WidgetOutputOperator operator;
    
    public TimeseriesInputPort(WidgetOutputOperator woo)
    {
      operator = woo;
    }
    
    @Override
    public void process(Pair<Long, Number> tuple)
    {
      HashMap<String, Number> timeseriesMap = Maps.newHashMapWithExpectedSize(2);
      timeseriesMap.put("timestamp", tuple.getFirst());
      timeseriesMap.put("value", tuple.getSecond());
      operator.input.process(new Pair<String, Object>(operator.timeSeriesPrefix + "_{\"type\":\"timeseries\",\"minValue\":" + operator.timeSeriesMin + 
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
      operator.input.process(new Pair<String, Object>(operator.topNPrefix + "_{\"type\":\"topN\",\"n\":" + operator.nInTopN + "}", result));
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
      operator.input.process(new Pair<String, Object>(operator.simplePrefix + "_{\"type\":\"simple\"}", tuple.toString()));
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
      operator.input.process(new Pair<String, Object>(operator.percentagePrefix + "_{\"type\":\"percentage\"}", tuple));
    }

    public PercentageInputPort setTopic(String topic)
    {
      operator.percentagePrefix = topic;
      return this;
    }
  }

}