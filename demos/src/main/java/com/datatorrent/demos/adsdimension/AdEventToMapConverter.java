package com.datatorrent.demos.adsdimension;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.google.common.collect.Maps;

import java.util.Map;

public class AdEventToMapConverter extends BaseOperator
{
  public transient final DefaultOutputPort<Map<String, Object>> out = new DefaultOutputPort<Map<String, Object>>();
  public transient final DefaultInputPort<AdInfo> in = new DefaultInputPort<AdInfo>()
  {
    @Override public void process(AdInfo tuple)
    {
      Map<String, Object> o = convert(tuple);
      out.emit(o);
    }
  };

  public Map<String, Object> convert(AdInfo adInfo)
  {
    Map<String, Object> map =Maps.newHashMap();
    map.put("pubId", new Integer(adInfo.publisherId));
    map.put("adUnit", new Integer(adInfo.adUnit));
    map.put("adId", new Integer(adInfo.advertiserId));
    map.put("timestamp", new Long(adInfo.timestamp));
    map.put("clicks", new Long(adInfo.clicks));
    return map;
  }
}
