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
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datatorrent.contrib.hdht.HDHTCodec;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.AbstractSinglePortHDSWriter;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregateEvent;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregator;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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

  public final transient DefaultOutputPort<TimeSeriesQueryResult> queryResult = new DefaultOutputPort<TimeSeriesQueryResult>();

  @InputPortFieldAnnotation(optional=true)
  public transient final DefaultInputPort<String> query = new DefaultInputPort<String>()
  {
    @Override public void process(String s)
    {
      try {
        LOG.debug("registering query {}", s);
        registerTimeSeriesQuery(s);
      } catch(Exception ex) {
        LOG.error("Unable to register query {}", s);
      }
    }
  };

  private transient final ByteBuffer valbb = ByteBuffer.allocate(8 * 4);
  private transient final ByteBuffer keybb = ByteBuffer.allocate(8 + 4 * 3);
  protected boolean debug = false;

  protected static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
  // in-memory aggregation before hitting WAL
  protected final SortedMap<Long, Map<AdInfoAggregateEvent, AdInfoAggregateEvent>> cache = Maps.newTreeMap();
  // TODO: should be aggregation interval count
  private int maxCacheSize = 5;

  private AdInfoAggregator aggregator;

  @VisibleForTesting
  protected transient final Map<String, TimeSeriesQuery> timeSeriesQueries = Maps.newConcurrentMap();
  private transient ObjectMapper mapper = null;
  private long defaultTimeWindow = TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES);

  public int getMaxCacheSize()
  {
    return maxCacheSize;
  }

  public void setMaxCacheSize(int maxCacheSize)
  {
    this.maxCacheSize = maxCacheSize;
  }

  public long getDefaultTimeWindow()
  {
    return defaultTimeWindow;
  }

  public void setDefaultTimeWindow(long defaultTimeWindow)
  {
    this.defaultTimeWindow = defaultTimeWindow;
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

  /**
   * Perform aggregation in memory until HDHT flush threshold is reached.
   * Avoids WAL writes for most of the aggregation.
   */
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
      // FIXME: lookup from store as the value may have been flushed previously.
      if (val == null) {
        valMap.put(event, event);
        return;
      } else {
        aggregator.aggregate(val, event);
      }
    }
  }

  @Override
  protected HDHTCodec<AdInfoAggregateEvent> getCodec()
  {
    return new AdInfoAggregateCodec();
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
    processTimeSeriesQueries();
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

  public static class AdInfoAggregateCodec extends KryoSerializableStreamCodec<AdInfoAggregateEvent> implements HDHTCodec<AdInfoAggregateEvent>
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
        private static final long serialVersionUID = 201411031407L;

  }

  protected void registerTimeSeriesQuery(String queryString) throws Exception
  {
    if (mapper == null) {
      mapper = new ObjectMapper();
      mapper.configure(org.codehaus.jackson.map.DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    QueryParameters queryParams = mapper.readValue(queryString, QueryParameters.class);

    if (queryParams.id == null) {
      return;
    }

    AdInfo.AdInfoAggregateEvent ae = mapper.convertValue(queryParams.keys, AdInfo.AdInfoAggregateEvent.class);

    long bucketKey = getBucketKey(ae);
    if (!(super.partitions == null || super.partitions.contains((int)bucketKey))) {
      //LOG.debug("Ignoring query for bucket {} when this partition serves {}", bucketKey, super.partitions);
      return;
    }

    TimeSeriesQuery query = new TimeSeriesQuery();
    query.id = queryParams.id;
    query.prototype = ae;
    query.windowCountdown = 30;

    query.endTime = queryParams.endTime;
    if (queryParams.endTime == 0) {
      // If endTime is not specified, then use current system time as end time.
      query.endTime = System.currentTimeMillis();
    }
    query.endTime = TimeUnit.MILLISECONDS.convert(query.intervalTimeUnit.convert(query.endTime, TimeUnit.MILLISECONDS), query.intervalTimeUnit);

    query.startTime = queryParams.startTime;
    if (queryParams.startTime == 0) {
      // If start time is not specified, return data for configured number of intervals (defaultTimeWindow)
      query.startTime = query.endTime - defaultTimeWindow;
    }
    query.startTime = TimeUnit.MILLISECONDS.convert(query.intervalTimeUnit.convert(query.startTime, TimeUnit.MILLISECONDS), query.intervalTimeUnit);

    // set query for each point in series
    query.prototype.timestamp = query.startTime;
    while (query.prototype.timestamp <= query.endTime) {
      Slice key = new Slice(getKey(query.prototype));
      HDSQuery q = super.queries.get(key);
      if (q == null) {
        q = new HDSQuery();
        q.bucketKey = bucketKey;
        q.key = key;
        super.addQuery(q);
      } else {
        // TODO: find out why we got null in first place
        if (q.result == null) {
          LOG.debug("Forcing refresh for {}", q);
          q.processed = false;
        }
      }
      q.keepAliveCount = query.windowCountdown;
      query.points.add(q);
      query.prototype.timestamp += query.intervalTimeUnit.toMillis(1);
    }
    LOG.debug("Queries: {}", query.points);
    timeSeriesQueries.put(query.id, query);
  }

  protected void processTimeSeriesQueries()
  {
    Iterator<Map.Entry<String, TimeSeriesQuery>> it = this.timeSeriesQueries.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, TimeSeriesQuery> e = it.next();
      TimeSeriesQuery rangeQuery = e.getValue();
      if (--rangeQuery.windowCountdown < 0) {
        LOG.debug("Removing expired query {}", rangeQuery);
        it.remove(); // query expired
        continue;
      }

      TimeSeriesQueryResult res = new TimeSeriesQueryResult();
      res.id = rangeQuery.id;
      res.countDown = rangeQuery.windowCountdown;
      res.data = Lists.newArrayListWithExpectedSize(rangeQuery.points.size());
      rangeQuery.prototype.timestamp = rangeQuery.startTime;
      for (HDSQuery query : rangeQuery.points) {
        // check in-flight memory store first
        Map<AdInfoAggregateEvent, AdInfoAggregateEvent> buffered = cache.get(rangeQuery.prototype.timestamp);
        if (buffered != null) {
          AdInfo.AdInfoAggregateEvent ae = buffered.get(rangeQuery.prototype);
          if (ae != null) {
            LOG.debug("Adding from aggregation buffer {}" + ae);
            res.data.add(ae);
            rangeQuery.prototype.timestamp += rangeQuery.intervalTimeUnit.toMillis(1);
            continue;
          }
        }
        // results from persistent store
        if (query.processed && query.result != null) {
          AdInfo.AdInfoAggregateEvent ae = super.codec.fromKeyValue(query.key, query.result);
          if (ae != null)
            res.data.add(ae);
        }
        rangeQuery.prototype.timestamp += TimeUnit.MINUTES.toMillis(1);
      }
      if (!res.data.isEmpty()) {
        LOG.debug("Emitting {} points for {}", res.data.size(), res.id);
        queryResult.emit(res);
      }
    }
  }

  static class TimeSeriesQuery
  {
    public String id;
    public int windowCountdown;
    public  AdInfo.AdInfoAggregateEvent prototype;
    public long startTime;
    public long endTime;
    public TimeUnit intervalTimeUnit = TimeUnit.MINUTES;
    private transient LinkedHashSet<HDSQuery> points = Sets.newLinkedHashSet();

    @Override public String toString()
    {
      return "TimeSeriesQuery{" +
          "id='" + id + '\'' +
          ", windowCountdown=" + windowCountdown +
          ", startTime=" + startTime +
          ", endTime=" + endTime +
          '}';
    }

    private static final long serialVersionUID = 201411031407L;
  }

  /**
   * Parameters for registration of query.
   */
  static class QueryParameters
  {
    public String id;
    public int numResults;
    public Map<String, String> keys;
    public long startTime;
    public long endTime;
  }

  static public class TimeSeriesQueryResult
  {
    public String id;
    public long countDown;
    public List<AdInfo.AdInfoAggregateEvent> data;
  }

}
