package com.datatorrent.apps.telecom.operator;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.contrib.machinedata.data.AverageData;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.TimeBucketKey;

public class CSSRKPIUnifier implements Unifier<KeyValPair<TimeBucketKey, AverageData>>
{

  private Map<TimeBucketKey, AverageData> aggregation = new HashMap<TimeBucketKey, AverageData>();
  public final transient DefaultOutputPort<KeyValPair<TimeBucketKey, AverageData>> outputPort = new DefaultOutputPort<KeyValPair<TimeBucketKey, AverageData>>();
  @Override
  public void beginWindow(long windowId)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void endWindow()
  {   
   for(Map.Entry<TimeBucketKey, AverageData> entry: aggregation.entrySet()){
     outputPort.emit(new KeyValPair<TimeBucketKey, AverageData>(entry.getKey(), entry.getValue()));
   }
   aggregation.clear();
  }

  @Override
  public void setup(OperatorContext context)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void process(KeyValPair<TimeBucketKey, AverageData> tuple)
  {
    if(aggregation.get(tuple.getKey()) == null){
      aggregation.put(tuple.getKey(),tuple.getValue());
    }else{
      AverageData data = aggregation.get(tuple.getKey());
      data.setCount(data.getCount() + tuple.getValue().getCount());
      data.setSum(data.getSum() + tuple.getValue().getSum());
    }
  }

}
