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
package com.datatorrent.demos.adsdimension;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hds.HDS;
import com.datatorrent.demos.adsdimension.AdInfo.AdInfoAggregateEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class HDSQueryOperator extends HDSOutputOperator
{
  public final transient DefaultOutputPort<HDSRangeQueryResult> queryResult = new DefaultOutputPort<HDSRangeQueryResult>();

  public transient final DefaultInputPort<String> query = new DefaultInputPort<String>()
  {
    @Override public void process(String s)
    {
      try {
        LOG.debug("registering query {}", s);
        registerQuery(s);
      } catch(Exception ex) {
        LOG.error("Unable to register query {}", s);
      }
    }
  };

  static class HDSRangeQuery
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
      return "HDSRangeQuery{" +
          "id='" + id + '\'' +
          ", windowCountdown=" + windowCountdown +
          ", startTime=" + startTime +
          ", endTime=" + endTime +
          '}';
    }
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

  static public class HDSRangeQueryResult
  {
    public String id;
    public long countDown;
    public List<AdInfo.AdInfoAggregateEvent> data;
  }

  @VisibleForTesting
  protected transient final Map<String, HDSRangeQuery> rangeQueries = Maps.newConcurrentMap();
  private transient ObjectMapper mapper = null;
  private long defaultTimeWindow = TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES);


  public long getDefaultTimeWindow()
  {
    return defaultTimeWindow;
  }

  public void setDefaultTimeWindow(long defaultTimeWindow)
  {
    this.defaultTimeWindow = defaultTimeWindow;
  }

  public void registerQuery(String queryString) throws Exception
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

    HDSRangeQuery query = new HDSRangeQuery();
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
      Slice key = HDS.SliceExt.toSlice(getKey(query.prototype));
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
    rangeQueries.put(query.id, query);
  }

  protected AdInfo.AdInfoAggregateEvent getAggregatesFromBytes(byte[] key, byte[] value)
  {
    if (key == null || value == null)
      return null;

    AdInfo.AdInfoAggregateEvent ae = new AdInfo.AdInfoAggregateEvent();
    if (debug) {
      return getAggregatesFromString(new String(key), new String(value));
    }

    java.nio.ByteBuffer bb = ByteBuffer.wrap(key);
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

  protected AdInfo.AdInfoAggregateEvent getAggregatesFromString(String key, String value)
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

  @Override
  public void endWindow()
  {
    super.endWindow();

    Iterator<Map.Entry<String, HDSRangeQuery>> it = this.rangeQueries.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, HDSRangeQuery> e = it.next();
      HDSRangeQuery rangeQuery = e.getValue();
      if (--rangeQuery.windowCountdown < 0) {
        LOG.debug("Removing expired query {}", rangeQuery);
        it.remove(); // query expired
        continue;
      }

      HDSRangeQueryResult res = new HDSRangeQueryResult();
      res.id = rangeQuery.id;
      res.countDown = rangeQuery.windowCountdown;
      res.data = Lists.newArrayListWithExpectedSize(rangeQuery.points.size());
      rangeQuery.prototype.timestamp = rangeQuery.startTime;
      for (HDSQuery query : rangeQuery.points) {
        // check in-flight memory store first
        Map<AdInfoAggregateEvent, AdInfoAggregateEvent> buffered = super.cache.get(rangeQuery.prototype.timestamp);
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
          AdInfo.AdInfoAggregateEvent ae = getAggregatesFromBytes(query.key.buffer, query.result);
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

  private static final Logger LOG = LoggerFactory.getLogger(HDSQueryOperator.class);
}
