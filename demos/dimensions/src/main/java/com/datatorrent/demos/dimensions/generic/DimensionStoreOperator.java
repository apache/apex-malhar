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
package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Dimensional Data Store
 * <p>
 * Operator receives dimensional aggregate data, and stores it to HDFS in by translating tuple data
 * into key/value structure with specified event schema.
 *
 * Schema can be specified as a JSON string with following keys.
 *
 *   fields: Map of all the field names and their types.  Supported types: java.lang.(Integer, Long, Float, Double, String)
 *   dimension: Array of dimensions with fields separated by colon, and time prefixed with time=.  Supported time units: MINUTES, HOURS, DAYS
 *   aggregates: Fields to aggregate for specified dimensions.  Aggregates types can include: sum, avg, min, max
 *   timestamp: Name of the timestamp field.  Data type should be Long with value in milliseconds since Jan 1, 1970 GMT.
 *
 * Example JSON schema for Ads demo:
 *
 *   {
 *     "fields": {"publisherId":"java.lang.Integer", "advertiserId":"java.lang.Integer", "adUnit":"java.lang.Integer", "clicks":"java.lang.Long", "price":"java.lang.Long", "cost":"java.lang.Double", "revenue":"java.lang.Double", "timestamp":"java.lang.Long"},
 *     "dimensions": ["time=MINUTES", "time=MINUTES:adUnit", "time=MINUTES:advertiserId", "time=MINUTES:publisherId", "time=MINUTES:advertiserId:adUnit", "time=MINUTES:publisherId:adUnit", "time=MINUTES:publisherId:advertiserId", "time=MINUTES:publisherId:advertiserId:adUnit"],
 *     "aggregates": { "clicks": "sum", "price": "sum", "cost": "sum", "revenue": "sum"},
 *     "timestamp": "timestamp"
 *   }
 *
 * @displayName HDHT Dimensional Store
 * @category Store
 * @tags storage, hdfs, dimensions, hdht
 *
 * @since 2.0.0
 */
public class DimensionStoreOperator extends AbstractSinglePortHDHTWriter<GenericAggregate>
{
  protected final SortedMap<Long, Map<GenericAggregate, GenericAggregate>> cache = Maps.newTreeMap();

  private int maxCacheSize = 5;
  private GenericAggregator aggregator;
  private transient EventSchema eventSchema;
  protected transient GenericAggregateSerializer serializer;

  public int getMaxCacheSize()
  {
    return maxCacheSize;
  }

  public void setMaxCacheSize(int maxCacheSize)
  {
    this.maxCacheSize = maxCacheSize;
  }

  public GenericAggregator getAggregator()
  {
    return aggregator;
  }

  public void setAggregator(GenericAggregator aggregator)
  {
    this.aggregator = aggregator;
  }

  public final transient DefaultOutputPort<HDSRangeQueryResult> queryResult = new DefaultOutputPort<HDSRangeQueryResult>();

  private String eventSchemaJSON = EventSchema.DEFAULT_SCHEMA_SALES;

  public String getEventSchemaJSON()
  {
    return eventSchemaJSON;
  }

  public void setEventSchemaJSON(String eventSchemaJSON)
  {
    this.eventSchemaJSON = eventSchemaJSON;
    setAggregator(new GenericAggregator(getEventSchema()));
  }

  // Initialize to use TFileImpl by default - to support simple App Builder drag-and-drop.
  {
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    setFileStore(hdsFile);
    hdsFile.setBasePath("DefaultDimensionStore");
  }

  public EventSchema getEventSchema() {
    if (eventSchema == null) {
      try {
        eventSchema = EventSchema.createFromJSON(eventSchemaJSON);
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to parse JSON input: " + eventSchemaJSON, e);
      }
    }
    return eventSchema;
  }


  public transient final DefaultInputPort<String> query = new DefaultInputPort<String>()
  {
    @Override
    public void process(String s)
    {
      try {
        LOG.debug("registering query {}", s);
        registerQuery(s);
      } catch(Exception ex) {
        LOG.error("Unable to register query {} with error", s, ex);
      }
    }
  };

  static class HDSRangeQuery
  {
    public String id;
    public int windowCountdown;
    public GenericAggregate prototype;
    public long startTime;
    public long endTime;
    public TimeUnit intervalTimeUnit = TimeUnit.MINUTES;
    private transient LinkedHashSet<HDSQuery> points = Sets.newLinkedHashSet();

    @Override
    public String toString()
    {
      return "TimeSeriesQuery{" +
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
    public List<Map<String, Object>> data;
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

    GenericAggregate ae = new GenericAggregate(eventSchema.convertQueryKeysToGenericEvent(queryParams.keys));

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
    query.prototype.setTimestamp(query.startTime);
    while (query.prototype.getTimestamp() <= query.endTime) {
      Slice key = new Slice(codec.getKeyBytes(query.prototype));
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
      query.prototype.setTimestamp(query.prototype.getTimestamp() + query.intervalTimeUnit.toMillis(1));
    }
    LOG.debug("Queries: {}", query.points);
    rangeQueries.put(query.id, query);
  }

  @Override
  protected HDHTCodec<GenericAggregate> getCodec()
  {
    return new GenericAggregateCodec();
  }

  @Override
  protected void processEvent(GenericAggregate aggr) throws IOException
  {
    // aggregate in-memory, flush to store in endWindow
    Map<GenericAggregate, GenericAggregate> valMap = cache.get(aggr.getTimestamp());
    if (valMap == null) {
      valMap = new HashMap<GenericAggregate, GenericAggregate>();
      valMap.put(aggr, aggr);
      cache.put(aggr.getTimestamp(), valMap);
    } else {
      GenericAggregate val = valMap.get(aggr);
      if (val == null) {
        valMap.put(aggr, aggr);
        return;
      } else {
        aggregator.aggregate(val, aggr);
      }
    }
  }

  @Override
  public void endWindow()
  {
    super.endWindow();

    // flush final aggregates to HDHT
    int expiredEntries = cache.size() - maxCacheSize;
    while(expiredEntries-- > 0){

      Map<GenericAggregate, GenericAggregate> vals = cache.remove(cache.firstKey());
      for (GenericAggregate ai: vals.values()) {
        try {
          super.processEvent(ai);
        } catch (IOException e) {
          LOG.warn("Error putting the value", e);
        }
      }
    }

    // process queries
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
      rangeQuery.prototype.setTimestamp(rangeQuery.startTime);
      for (HDSQuery query : rangeQuery.points) {
        // check in-flight memory store first
        Map<GenericAggregate, GenericAggregate> buffered = this.cache.get(rangeQuery.prototype.getTimestamp());
        if (buffered != null) {
          GenericAggregate ga = buffered.get(rangeQuery.prototype);
          if (ga != null) {
            LOG.debug("Adding from aggregation buffer {}" + ga);
            res.data.add(eventSchema.convertAggregateEventToMap(ga));
            rangeQuery.prototype.setTimestamp(rangeQuery.prototype.getTimestamp() + rangeQuery.intervalTimeUnit.toMillis(1));
            continue;
          }
        }
        // results from persistent store
        if (query.processed && query.result != null) {
          GenericAggregate ga = codec.fromKeyValue(query.key, query.result);
          if (ga.aggregates != null)
            res.data.add(eventSchema.convertAggregateEventToMap(ga));
        }
        rangeQuery.prototype.setTimestamp(rangeQuery.prototype.getTimestamp() + rangeQuery.intervalTimeUnit.toMillis(1));
      }
      if (!res.data.isEmpty()) {
        LOG.debug("Emitting {} points for {}", res.data.size(), res.id);
        queryResult.emit(res);
      }
    }
  }



  @Override public void setup(Context.OperatorContext arg0)
  {
    super.setup(arg0);
    this.serializer = new GenericAggregateSerializer(getEventSchema());
    setAggregator(new GenericAggregator(getEventSchema()));
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionStoreOperator.class);
}
