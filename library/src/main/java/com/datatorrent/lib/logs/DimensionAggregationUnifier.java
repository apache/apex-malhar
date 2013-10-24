package com.datatorrent.lib.logs;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

public class DimensionAggregationUnifier implements Operator
{

  private Map<String, Map<String, MutableDouble>> dataMap = new HashMap<String, Map<String, MutableDouble>>();

  public final transient DefaultOutputPort<Map<String, DimensionObject<String>>> output = new DefaultOutputPort<Map<String, DimensionObject<String>>>();
  public final transient DefaultInputPort<Map<String, DimensionObject<String>>> input = new DefaultInputPort<Map<String, DimensionObject<String>>>() {

    @Override
    public void process(Map<String, DimensionObject<String>> tuple)
    {
      for (Map.Entry<String, DimensionObject<String>> e : tuple.entrySet()) {
        Map<String, MutableDouble> obj = dataMap.get(e.getKey());
        DimensionObject<String> eObj = e.getValue();
        if (obj == null) {
          obj = new HashMap<String, MutableDouble>();
          obj.put(eObj.getVal(), new MutableDouble(eObj.getCount()));
          dataMap.put(e.getKey(), obj);
        } else {
          MutableDouble n = obj.get(eObj.getVal());
          if (n == null) {
            obj.put(eObj.getVal(), new MutableDouble(eObj.getCount()));
          } else {
            n.add(eObj.getCount());
          }
        }
      }

    }
  };

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
  public void beginWindow(long windowId)
  {
    dataMap = new HashMap<String, Map<String, MutableDouble>>();
    // TODO Auto-generated method stub

  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<String, Map<String, MutableDouble>> e : dataMap.entrySet()) {
      for (Map.Entry<String, MutableDouble> dimensionValObj : e.getValue().entrySet()) {
        Map<String, DimensionObject<String>> outputData = new HashMap<String, DimensionObject<String>>();
        outputData.put(e.getKey(), new DimensionObject<String>(dimensionValObj.getValue(), dimensionValObj.getKey()));
        output.emit(outputData);
      }
    }
    dataMap.clear();
  }

}
