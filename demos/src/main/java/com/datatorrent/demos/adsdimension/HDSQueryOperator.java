package com.datatorrent.demos.adsdimension;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HDSQueryOperator extends HDSOutputOperator
{
  public final transient DefaultOutputPort<HDSRangeQueryResult> queryResult =
      new DefaultOutputPort<HDSRangeQueryResult>();

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
    public int numResults;
    public long endTime;
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
  private ObjectMapper mapper = null;

  public void registerQuery(String queryString) throws Exception
  {
    if (mapper == null) {
      mapper = new ObjectMapper();
      mapper.configure(
          org.codehaus.jackson.map.DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    QueryParameters queryParams =
        mapper.readValue(queryString, QueryParameters.class);

    if (queryParams.id == null) {
      return;
    }

    AdInfo.AdInfoAggregateEvent key = mapper.convertValue(queryParams.keys, AdInfo.AdInfoAggregateEvent.class);

    HDSRangeQuery query = new HDSRangeQuery();
    query.id = queryParams.id;
    query.prototype = key;
    query.windowCountdown = 30;
    query.startTime = queryParams.startTime;
    query.numResults = queryParams.numResults;
    query.endTime = queryParams.endTime;

    rangeQueries.put(query.id, query);
  }


  public void processAllQueries() {

    for(HDSRangeQuery query : rangeQueries.values()) {
      // TODO: Generate multiple queries, currently we are
      // generating single point query for start time only.
      HDSQuery q = new HDSQuery();
      query.prototype.timestamp = query.startTime;
      q.bucketKey = getBucketKey(query.prototype);
      q.key = getKey(query.prototype);
      addQuery(q);
    }

    for(HDSQuery q : queries.values())
    {
      processQuery(q);
    }
  }

  private transient final ByteBuffer valbb = ByteBuffer.allocate(8 * 4);
  private transient final ByteBuffer keybb = ByteBuffer.allocate(8 + 4 * 3);

  private AdInfo.AdInfoAggregateEvent getAggregatesFromBytes(byte[] key, byte[] value) throws IOException
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

  @Override protected void emitQueryResult(HDSQuery query)
  {
    AdInfo.AdInfoAggregateEvent ae;
    try {
      ae = getAggregatesFromBytes(query.key, query.result);
    } catch (Exception ex) {
      System.out.println("parse exception");
      ex.printStackTrace();
      System.out.println(ex.getMessage());
      return;
    }

    // TODO: track the top level query, and add to its result set.
    // For now generating a fake result.
    HDSRangeQueryResult res = new HDSRangeQueryResult();
    res.id = "";
    res.data = Lists.newArrayList();
    res.data.add(ae);
    queryResult.emit(res);
  }

  private static transient final Logger LOG = LoggerFactory.getLogger(HDSQueryOperator.class);
}
