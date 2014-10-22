/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.dimensions.ads;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hds.AbstractSinglePortHDSWriter;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregateEvent;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregator;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.google.common.collect.Maps;

/**
 * AdsDimension Store Operator
 *
 * @displayName Dimensional Store
 * @category Store
 * @tags storage, hdfs, dimensions, hdht
 */
public class AdsDimensionStoreOperator extends AbstractSinglePortHDSWriter<AdInfoAggregateEvent>
{
  private static final Logger LOG = LoggerFactory.getLogger(AdsDimensionStoreOperator.class);

  private transient final ByteBuffer valbb = ByteBuffer.allocate(8 * 4);

  private transient final ByteBuffer keybb = ByteBuffer.allocate(8 + 4 * 3);

  protected boolean debug = false;

  protected static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

  protected final SortedMap<Long, Map<AdInfoAggregateEvent, AdInfoAggregateEvent>> cache = Maps.newTreeMap();

  // TODO: should be aggregation interval count
  private int maxCacheSize = 5;

  private AdInfoAggregator aggregator;

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
    // flush final aggregates
    int expiredEntries = cache.size() - maxCacheSize;
    while(expiredEntries-- > 0){

      Map<AdInfoAggregateEvent, AdInfoAggregateEvent> vals = cache.remove(cache.firstKey());
      for (Entry<AdInfoAggregateEvent, AdInfoAggregateEvent> en : vals.entrySet()) {
        AdInfoAggregateEvent ai = en.getValue();
        try {
          put(getBucketKey(ai), new Slice(getKey(ai)), getValue(ai));
        } catch (IOException e) {
          LOG.warn("Error putting the value", e);
        }
      }
    }
    super.endWindow();
  }

  protected byte[] getKey(AdInfo event)
  {
    if (debug) {
      StringBuilder keyBuilder = new StringBuilder(32);
      keyBuilder.append(sdf.format(new Date(event.timestamp)));
      keyBuilder.append("|publisherId:").append(event.publisherId);
      keyBuilder.append("|advertiserId:").append(event.advertiserId);
      keyBuilder.append("|adUnit:").append(event.adUnit);
      return keyBuilder.toString().getBytes();
    }

    byte[] data = new byte[8 + 4 * 3];
    keybb.rewind();
    keybb.putLong(event.getTimestamp());
    keybb.putInt(event.getPublisherId());
    keybb.putInt(event.getAdvertiserId());
    keybb.putInt(event.getAdUnit());
    keybb.rewind();
    keybb.get(data);
    return data;
  }

  protected byte[] getValue(AdInfoAggregateEvent event)
  {
    if (debug) {
      StringBuilder keyBuilder = new StringBuilder(32);
      keyBuilder.append("|clicks:").append(event.clicks);
      keyBuilder.append("|cost:").append(event.cost);
      keyBuilder.append("|impressions:").append(event.impressions);
      keyBuilder.append("|revenue:").append(event.revenue);
      return keyBuilder.toString().getBytes();
    }
    byte[] data = new byte[8 * 4];
    valbb.rewind();
    valbb.putLong(event.clicks);
    valbb.putDouble(event.cost);
    valbb.putLong(event.impressions);
    valbb.putDouble(event.revenue);
    valbb.rewind();
    valbb.get(data);
    return data;
  }

  private AdInfo.AdInfoAggregateEvent getAggregateFromString(String key, String value)
  {
    AdInfo.AdInfoAggregateEvent ae = new AdInfo.AdInfoAggregateEvent();
    Pattern p = Pattern.compile("([^|]*)\\|publisherId:(\\d+)\\|advertiserId:(\\d+)\\|adUnit:(\\d+)");
    Matcher m = p.matcher(key);
    m.find();
    try {
      Date date = sdf.parse(m.group(1));
      ae.timestamp = date.getTime();
    } catch (Exception ex) {
      ae.timestamp = 0;
    }
    ae.publisherId = Integer.valueOf(m.group(2));
    ae.advertiserId = Integer.valueOf(m.group(3));
    ae.adUnit = Integer.valueOf(m.group(4));

    p = Pattern.compile("\\|clicks:(.*)\\|cost:(.*)\\|impressions:(.*)\\|revenue:(.*)");
    m = p.matcher(value);
    m.find();
    ae.clicks = Long.valueOf(m.group(1));
    ae.cost = Double.valueOf(m.group(2));
    ae.impressions = Long.valueOf(m.group(3));
    ae.revenue = Double.valueOf(m.group(4));
    return ae;
  }

  protected AdInfo.AdInfoAggregateEvent bytesToAggregate(Slice key, byte[] value)
  {
    if (key == null || value == null)
      return null;

    AdInfo.AdInfoAggregateEvent ae = new AdInfo.AdInfoAggregateEvent();
    if (debug) {
      return getAggregateFromString(new String(key.buffer, key.offset, key.length), new String(value));
    }

    java.nio.ByteBuffer bb = ByteBuffer.wrap(key.buffer, key.offset, key.length);
    ae.timestamp = bb.getLong();
    ae.publisherId = bb.getInt();
    ae.advertiserId = bb.getInt();
    ae.adUnit = bb.getInt();

    bb = ByteBuffer.wrap(value);
    ae.clicks = bb.getLong();
    ae.cost = bb.getDouble();
    ae.impressions = bb.getLong();
    ae.revenue = bb.getDouble();
    return ae;
  }

  @Override
  protected void processEvent(AdInfoAggregateEvent event) throws IOException
  {
    Map<AdInfoAggregateEvent, AdInfoAggregateEvent> valMap = cache.get(event.getTimestamp());
    if (valMap == null) {
      valMap = new HashMap<AdInfoAggregateEvent, AdInfoAggregateEvent>();
      valMap.put(event, event);
      cache.put(event.getTimestamp(), valMap);
    } else {
      AdInfoAggregateEvent val = valMap.get(event);
      if (val == null) {
        valMap.put(event, event);
        return;
      } else {
        aggregator.aggregate(val, event);
      }
    }
  }

  @Override
  protected Class<? extends HDSCodec<AdInfoAggregateEvent>> getCodecClass()
  {
    return AdInfoAggregateCodec.class;
  }

  public static class AdInfoAggregateCodec extends KryoSerializableStreamCodec<AdInfoAggregateEvent> implements AbstractSinglePortHDSWriter.HDSCodec<AdInfoAggregateEvent>
  {
    public AdsDimensionStoreOperator operator;

    @Override
    public byte[] getKeyBytes(AdInfoAggregateEvent aggr)
    {
      return operator.getKey(aggr);
    }

    @Override
    public byte[] getValueBytes(AdInfoAggregateEvent aggr)
    {
      return operator.getValue(aggr);
    }

    @Override
    public AdInfoAggregateEvent fromKeyValue(Slice key, byte[] value)
    {
      if (key == null || value == null)
        return null;

      AdInfo.AdInfoAggregateEvent ae = new AdInfo.AdInfoAggregateEvent();
      if (operator.debug) {
        return operator.getAggregateFromString(new String(key.buffer, key.offset, key.length), new String(value));
      }

      java.nio.ByteBuffer bb = ByteBuffer.wrap(key.buffer, key.offset, key.length);
      ae.timestamp = bb.getLong();
      ae.publisherId = bb.getInt();
      ae.advertiserId = bb.getInt();
      ae.adUnit = bb.getInt();

      bb = ByteBuffer.wrap(value);
      ae.clicks = bb.getLong();
      ae.cost = bb.getDouble();
      ae.impressions = bb.getLong();
      ae.revenue = bb.getDouble();
      return ae;
    }

    @Override
    public int getPartition(AdInfoAggregateEvent t)
    {
      final int prime = 31;
      int result = 1;
      result = prime * result + t.adUnit;
      result = prime * result + t.advertiserId;
      result = prime * result + t.publisherId;
      return result;
    }
  }

  public AdInfoAggregator getAggregator()
  {
    return aggregator;
  }

  public void setAggregator(AdInfoAggregator aggregator)
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

}
