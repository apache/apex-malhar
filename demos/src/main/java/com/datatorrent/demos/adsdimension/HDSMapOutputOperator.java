package com.datatorrent.demos.adsdimension;

import com.datatorrent.contrib.hds.AbstractSinglePortHDSWriter;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;


public class HDSMapOutputOperator extends AbstractSinglePortHDSWriter<MapAggregate>
{
  private static final Logger LOG = LoggerFactory.getLogger(HDSOutputOperator.class);

  protected boolean debug = false;

  protected final SortedMap<Long, Map<MapAggregate, MapAggregate>> cache = Maps.newTreeMap();

  // TODO: should be aggregation interval count
  private int maxCacheSize = 5;

  private MapAggregator aggregator;

  public int getMaxCacheSize()
  {
    return maxCacheSize;
  }

  public void setMaxCacheSize(int maxCacheSize)
  {
    this.maxCacheSize = maxCacheSize;
  }

  @Override
  public void endWindow()
  {
    int expiredEntries = cache.size() - maxCacheSize;
    while(expiredEntries-- > 0){

      Map<MapAggregate, MapAggregate> vals = cache.remove(cache.firstKey());
      for (Map.Entry<MapAggregate, MapAggregate> en : vals.entrySet()) {
        MapAggregate ai = en.getValue();
        try {
          super.processEvent(ai);
        } catch (IOException e) {
          LOG.warn("Error putting the value", e);
        }
      }
    }

    super.endWindow();
  }

  @Override protected void processEvent(MapAggregate aggr) throws IOException
  {
    Map<MapAggregate, MapAggregate> valMap = cache.get(aggr.getTimestamp());
    if (valMap == null) {
      valMap = new HashMap<MapAggregate, MapAggregate>();
      valMap.put(aggr, aggr);
      cache.put(aggr.getTimestamp(), valMap);
    } else {
      MapAggregate val = valMap.get(aggr);
      if (val == null) {
        valMap.put(aggr, aggr);
        return;
      } else {
        aggregator.aggregate(val, aggr);
      }
    }
  }

  public MapAggregator getAggregator()
  {
    return aggregator;
  }


  public void setAggregator(MapAggregator aggregator)
  {
    this.aggregator = aggregator;
  }


  public boolean isDebug()
  {
    return debug;
  }


  public void setDebug(boolean debug)
  {
    this.debug = debug;
  }

  @Override protected Class<? extends HDSCodec<MapAggregate>> getCodecClass()
  {
    return MapAggregateCodec.class;
  }
}
