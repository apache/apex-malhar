/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.summit.ads;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.KeyValPair;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableDouble;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class BucketOperator extends BaseOperator
{

  private long windowWidth;
  private long currentWindowId;

  private HashMap<AggrKey, Map<String, MutableDouble>> aggrMap;

  @OutputPortFieldAnnotation(name = "outputPort", optional = false)
  public final transient DefaultOutputPort<KeyValPair<AggrKey, Map<String, MutableDouble>>> outputPort = new DefaultOutputPort<KeyValPair<AggrKey, Map<String, MutableDouble>>>(this);

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    windowWidth = context.attrValue(DAGContext.STREAMING_WINDOW_SIZE_MILLIS, 500);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    currentWindowId = windowId;
    aggrMap = new HashMap<AggrKey, Map<String, MutableDouble>>();
  }

  @Override
  public void endWindow()
  {
    //outputPort.emit(aggrMap);
    for (Map.Entry<AggrKey, Map<String, MutableDouble>> entry : aggrMap.entrySet()) {
      //Map<AggrKey, Map<String, MutableDouble>> map = new HashMap<AggrKey, Map<String, MutableDouble>>();
      //map.put(entry.getKey(), entry.getValue());
      //outputPort.emit(map);
      outputPort.emit(new KeyValPair<AggrKey,Map<String, MutableDouble>>(entry.getKey(), entry.getValue()));
    }
  }

  @Override
  public void teardown()
  {
    super.teardown(); //To change body of generated methods, choose Tools | Templates.
  }

  private long getTime() {
    return (currentWindowId >>> 32) * 1000 + windowWidth * (currentWindowId & 0xffffffffL);
  }

  @InputPortFieldAnnotation(name = "inputPort", optional = false)
  public transient DefaultInputPort<AdInfo>  inputPort = new DefaultInputPort<AdInfo>(this) {

    @Override
    public void process(AdInfo tuple)
    {
      Calendar calendar = Calendar.getInstance();
      calendar.setTimeInMillis(tuple.getTimestamp());
      AggrKey aggrKey = new AggrKey(calendar,AggrKey.TIMESPEC_MINUTE_SPEC, tuple.getPublisherId(), tuple.getAdvertiserId(), tuple.getAdUnit());
      Map<String, MutableDouble> map = aggrMap.get(aggrKey);
      if (map == null) {
        map = new HashMap<String, MutableDouble>();
        aggrMap.put(aggrKey, map);
      }
      if (tuple.isClick()) {
        updateVal(map, "0", 1);
        updateVal(map, "1", tuple.getValue());
        updateVal(map, "2", 0.0);
        updateVal(map, "3", 1);
        updateVal(map, "4", 0);
      } else {
        updateVal(map, "0", 1);
        updateVal(map, "1", 0.0);
        updateVal(map, "2", tuple.getValue());
        updateVal(map, "3", 0);
        updateVal(map, "4", 1);
      }
    }

    void updateVal(Map<String, MutableDouble> map, String key, Number val) {
      MutableDouble cval = map.get(key);
      if (cval == null) {
        map.put(key, new MutableDouble(val));
      }else {
        cval.add(val);
      }
    }

  };

}
