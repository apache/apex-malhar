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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
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
    private transient Set<HDSQuery> results = Sets.newHashSet();
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
  private transient final Map<HDSQuery, HDSRangeQuery> queryGrouping = Maps.newHashMap();
  private ObjectMapper mapper = null;

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

    AdInfo.AdInfoAggregateEvent key = mapper.convertValue(queryParams.keys, AdInfo.AdInfoAggregateEvent.class);

    HDSRangeQuery query = new HDSRangeQuery();
    query.id = queryParams.id;
    query.prototype = key;
    query.windowCountdown = 30;
    query.startTime = queryParams.startTime;
    //query.numResults = queryParams.numResults;
    query.endTime = queryParams.endTime;

    rangeQueries.put(query.id, query);

    // TODO: set query for each point in series
    HDSQuery q = new HDSQuery();
    query.prototype.timestamp = query.startTime;
    q.bucketKey = getBucketKey(query.prototype);
    q.key = getKey(query.prototype);
    super.addQuery(q);
    this.queryGrouping.put(q, query);

  }

  private AdInfo.AdInfoAggregateEvent getAggregatesFromBytes(byte[] key, byte[] value)
  {
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


  private AdInfo.AdInfoAggregateEvent getAggregatesFromString(String key, String value)
  {
    LOG.debug("converting key {} value is {}", key, value);
    AdInfo.AdInfoAggregateEvent ae = new AdInfo.AdInfoAggregateEvent();
    String str = new String(key);
    Pattern p = Pattern.compile("(.*)|publisherId:(\\d+)|advertiserId:(\\d+)|adUnit:(\\d+)");
    Matcher m = p.matcher(str);
    try {
      Date date = sdf.parse(m.group(1));
      ae.timestamp = date.getTime();
    } catch (Exception ex) {
      ae.timestamp = 0;
    }
    ae.publisherId = Integer.valueOf(m.group(2));
    ae.advertiserId = Integer.valueOf(m.group(3));
    ae.adUnit = Integer.valueOf(m.group(4));

    str = new String(value);
    p = Pattern.compile("|clicks:(.*)|cost:(.*)|impressions:(.*)|revenue:(.*)");
    m = p.matcher(str);
    ae.clicks = Integer.valueOf(m.group(1));
    ae.cost = Float.valueOf(m.group(2));
    ae.impressions = Long.valueOf(m.group(3));
    ae.revenue = Long.valueOf(m.group(4));
    return ae;
  }

  @Override
  protected void emitQueryResult(HDSQuery query)
  {
    HDSRangeQuery rangeQuery = this.queryGrouping.get(query);
    if (rangeQuery == null) {
      LOG.debug("Did not find the series for {}", query);
      return;
    }
    rangeQuery.results.add(query);
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

      if (!rangeQuery.results.isEmpty()) {
        HDSRangeQueryResult res = new HDSRangeQueryResult();
        res.id = rangeQuery.id;
        res.countDown = rangeQuery.windowCountdown;
        res.data = Lists.newArrayListWithExpectedSize(rangeQuery.results.size());
        for (HDSQuery query : rangeQuery.results) {
          AdInfo.AdInfoAggregateEvent ae = getAggregatesFromBytes(query.key, query.result);
          res.data.add(ae);
        }
        queryResult.emit(res);
        rangeQuery.results.clear();
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(HDSQueryOperator.class);
}
