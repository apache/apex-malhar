/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.apps.logstream;

import java.util.*;
import java.util.Map.Entry;

import javax.validation.ValidationException;
import javax.validation.constraints.NotNull;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.logs.DimensionObject;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;

import com.datatorrent.apps.logstream.LogstreamUtil.AggregateOperation;
import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;
import com.datatorrent.apps.logstream.PropertyRegistry.PropertyRegistry;
import com.datatorrent.common.util.DTThrowable;

/**
 * Partitionable dimension operator.
 * Computes specified operators on specified values for specified dimension combinations.
 * Each partition works on a filtered tuple of a log type.
 */
public class DimensionOperator extends BaseOperator implements Partitioner<DimensionOperator>
{
  /**
   * key: timebucket|timestamp|recordtype|filter|dimensionId|value.operationType
   * value: DimensionObject
   */
  public final transient DefaultOutputPort<Map<String, DimensionObject<String>>> aggregationsOutput = new DefaultOutputPort<Map<String, DimensionObject<String>>>()
  {
    @Override
    public Unifier<Map<String, DimensionObject<String>>> getUnifier()
    {
      DimensionOperatorUnifier unifier = new DimensionOperatorUnifier();
      return unifier;
    }

  };
  public final transient DefaultInputPort<Map<String, Object>> in = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      processTuple(tuple);
    }

    @Override
    public Class<? extends StreamCodec<Map<String, Object>>> getStreamCodec()
    {
      return DimensionOperatorStreamCodec.class;
    }

  };
  @NotNull
  private PropertyRegistry<String> registry;
  private static final Logger logger = LoggerFactory.getLogger(DimensionOperator.class);
  private String timeKeyName;
  private long windowWidth = 500;
  private long currentWindowId;
  private transient TimeZone timeZone = TimeZone.getTimeZone("GMT");
  private transient Calendar calendar = new GregorianCalendar(timeZone);
  private int timeBucketFlags;
  private final Map<String, Map<String, Map<AggregateOperation, Number>>> cacheObject = new HashMap<String, Map<String, Map<AggregateOperation, Number>>>();
  private final HashMap<String, Number> recordType = new HashMap<String, Number>();
  private HashMap<Integer, HashMap<String, HashSet<AggregateOperation>>> valueOperations = new HashMap<Integer, HashMap<String, HashSet<AggregateOperation>>>();
  private HashMap<Integer, ArrayList<Integer>> dimensionCombinationList = new HashMap<Integer, ArrayList<Integer>>();
  private transient boolean firstTuple = true;
  private ArrayList<Integer> dimensionCombinations;
  private HashMap<String, HashSet<AggregateOperation>> valueOperationTypes;
  List<String> outTimeBuckets; // represents the list of timebuckets to be emitted when the time bucket changes

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if (context != null) {
      windowWidth = context.getValue(DAGContext.STREAMING_WINDOW_SIZE_MILLIS);
    }

    LogstreamPropertyRegistry.setInstance(registry);
  }

  /**
   * Does dimensional computations for each incoming tuple and populates the cache with the computations
   *
   * @param tuple
   */
  protected void processTuple(Map<String, Object> tuple)
  {
    long time;
    if (timeKeyName != null) {
      time = (Long)tuple.get(timeKeyName);
    }
    else {
      time = LogstreamUtil.extractTime(currentWindowId, windowWidth);
    }
    List<String> timeBucketList = getTimeBucketList(time);

    if (firstTuple) {
      // populate record type
      extractType(tuple);
      outTimeBuckets = new ArrayList<String>(timeBucketList);

      // create all dimension combinations if not specified by user
      if (!dimensionCombinationList.containsKey(recordType.get(LogstreamUtil.LOG_TYPE))) {
        createAllDimensionCombinations();
      }

      dimensionCombinations = dimensionCombinationList.get(recordType.get(LogstreamUtil.LOG_TYPE));
      valueOperationTypes = valueOperations.get(recordType.get(LogstreamUtil.LOG_TYPE));
      firstTuple = false;
    }

    // temporary validation to ensure that unexpected records do not appear in any partition
    Number receivedLogType = (Number)tuple.get(LogstreamUtil.LOG_TYPE);
    Number receivedFilter = (Number)tuple.get(LogstreamUtil.FILTER);

    Number expectedLogType = recordType.get(LogstreamUtil.LOG_TYPE);
    Number expectedFilter = recordType.get(LogstreamUtil.FILTER);

    if (!receivedLogType.equals(expectedLogType) || !receivedFilter.equals(expectedFilter)) {
      logger.error("Unexpected tuple");
      logger.error("expected log type = {} received = {}", expectedLogType, receivedLogType);
      logger.error("expected filter = {} received = {}", expectedFilter, receivedFilter);
    }
    else {
      for (String timeBucket : timeBucketList) {
        for (Integer dimensionCombinationId : dimensionCombinations) {
          String dimensionCombination = registry.lookupValue(dimensionCombinationId);
          String[] dimensions = dimensionCombination.split(":");

          String dimValueName = new String();
          boolean isBadTuple = false;
          if (dimensions != null) {
            for (String dimension : dimensions) {
              Object dimVal = tuple.get(dimension);
              if (dimVal == null) {
                logger.error("dimension \"{}\" not found in tuple", dimension);
                isBadTuple = true;
                continue;
              }
              if (!dimValueName.isEmpty()) {
                dimValueName += ",";
              }
              dimValueName += tuple.get(dimension).toString();
            }
          }

          if (!isBadTuple) {
            for (Entry<String, HashSet<AggregateOperation>> entry : valueOperationTypes.entrySet()) {
              String valueKeyName = entry.getKey();
              Object value = tuple.get(valueKeyName);
              Number numberValue = LogstreamUtil.extractNumber(value);
              doComputations(timeBucket, dimensionCombinationId, dimValueName, valueKeyName, numberValue);
            }
          }
        }
      }
    }
  }

  /**
   * returns list of time buckets for the given timestamp
   *
   * @param time
   * @return
   */
  protected List<String> getTimeBucketList(long time)
  {

    calendar.setTimeInMillis(time);

    List<String> timeBucketList = new ArrayList<String>();

    if ((timeBucketFlags & LogstreamUtil.TIMEBUCKET_YEAR) != 0) {
      timeBucketList.add(String.format("Y|%04d", calendar.get(Calendar.YEAR)));
    }
    if ((timeBucketFlags & LogstreamUtil.TIMEBUCKET_MONTH) != 0) {
      timeBucketList.add(String.format("M|%04d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1));
    }
    if ((timeBucketFlags & LogstreamUtil.TIMEBUCKET_WEEK) != 0) {
      timeBucketList.add(String.format("W|%04d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.WEEK_OF_YEAR)));
    }
    if ((timeBucketFlags & LogstreamUtil.TIMEBUCKET_DAY) != 0) {
      timeBucketList.add(String.format("D|%04d%02d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH)));
    }
    if ((timeBucketFlags & LogstreamUtil.TIMEBUCKET_HOUR) != 0) {
      timeBucketList.add(String.format("h|%04d%02d%02d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY)));
    }
    if ((timeBucketFlags & LogstreamUtil.TIMEBUCKET_MINUTE) != 0) {
      timeBucketList.add(String.format("m|%04d%02d%02d%02d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE)));
    }
    if ((timeBucketFlags & LogstreamUtil.TIMEBUCKET_SECOND) != 0) {
      timeBucketList.add(String.format("s|%04d%02d%02d%02d%02d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND)));
    }

    return timeBucketList;
  }

  /**
   * Does computations for the given dimension and its value names on the given value key name
   *
   * @param timeBucket time bucket
   * @param dimensionCombinationId id of dimension combination
   * @param dimValueName values of the dimension combination
   * @param valueKeyName name of the value key on which operations are performed
   * @param value value of the value key
   */
  private void doComputations(String timeBucket, Integer dimensionCombinationId, String dimValueName, String valueKeyName, Number value)
  {
    StringBuilder sb = new StringBuilder();
    sb.append(timeBucket).append("|").append(recordType.get(LogstreamUtil.LOG_TYPE)).append("|").append(recordType.get(LogstreamUtil.FILTER)).append("|").append(dimensionCombinationId).append("|").append(valueKeyName);

    //final key format --> timebucket|type|filter|dimId|val
    //eg: m|201311230108|1|4|10|bytes
    String key = sb.toString();

    Map<AggregateOperation, Number> aggregations;

    if (cacheObject.containsKey(key)) {
      Map<String, Map<AggregateOperation, Number>> dimValueNames = cacheObject.get(key);
      if (dimValueNames.containsKey(dimValueName)) {
        aggregations = dimValueNames.get(dimValueName);
      }
      else {
        aggregations = new HashMap<AggregateOperation, Number>();
        for (AggregateOperation aggregationType : valueOperationTypes.get(valueKeyName)) {
          aggregations.put(aggregationType, new MutableDouble(0));
        }

        dimValueNames.put(dimValueName, aggregations);
      }
    }
    else {
      Map<String, Map<AggregateOperation, Number>> newDimValueNames = new HashMap<String, Map<AggregateOperation, Number>>();
      aggregations = new HashMap<AggregateOperation, Number>();
      for (AggregateOperation aggregationType : valueOperationTypes.get(valueKeyName)) {
        aggregations.put(aggregationType, new MutableDouble(0));
      }
      newDimValueNames.put(dimValueName, aggregations);
      cacheObject.put(key, newDimValueNames);
    }

    if (aggregations.containsKey(AggregateOperation.SUM)) {
      MutableDouble aggrVal = (MutableDouble)aggregations.get(AggregateOperation.SUM);
      aggrVal.add(value);
    }

    if (aggregations.containsKey(AggregateOperation.COUNT)) {
      MutableDouble aggrVal = (MutableDouble)aggregations.get(AggregateOperation.COUNT);
      aggrVal.add(1);
    }

    if (aggregations.containsKey(AggregateOperation.AVERAGE)) {
      double avgVal = aggregations.get(AggregateOperation.AVERAGE).doubleValue();
      double countVal = aggregations.get(AggregateOperation.COUNT).doubleValue();
      double newAvg = ((avgVal * (countVal - 1)) + value.doubleValue()) / countVal;
      aggregations.put(AggregateOperation.AVERAGE, new MutableDouble(newAvg));
    }

  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    currentWindowId = windowId;
  }

  @Override
  public void endWindow()
  {
    if (outTimeBuckets == null || outTimeBuckets.isEmpty()) {
      return;
    }

    long time = LogstreamUtil.extractTime(currentWindowId, windowWidth);

    // get time buckets for current window id
    List<String> timeBucketList = getTimeBucketList(time);

    // get list of timebuckets to be emitted and replace them in outTimeBuckets with next time bucket to be emitted
    ArrayList<String> emitTimeBucketList = new ArrayList<String>();
    for (int i = 0; i < timeBucketList.size(); i++) {
      String timeBucket = timeBucketList.get(i);
      if (!timeBucket.equals(outTimeBuckets.get(i))) {
        emitTimeBucketList.add(outTimeBuckets.get(i));
        outTimeBuckets.set(i, timeBucket);
      }
    }

    // emit the computations for each time bucket in emitTimeBucketList and remove those buckets from the cache since they are now already processed
    if (!emitTimeBucketList.isEmpty()) {
      ArrayList<String> obsoleteKeys = new ArrayList<String>();
      for (String outTimeStr : emitTimeBucketList) {
        HashMap<String, DimensionObject<String>> outputAggregationsObject;

        for (Entry<String, Map<String, Map<AggregateOperation, Number>>> keys : cacheObject.entrySet()) {
          String key = keys.getKey();
          if (key.startsWith(outTimeStr)) {
            Map<String, Map<AggregateOperation, Number>> dimValues = keys.getValue();

            for (Entry<String, Map<AggregateOperation, Number>> dimValue : dimValues.entrySet()) {
              String dimValueName = dimValue.getKey();
              Map<AggregateOperation, Number> operations = dimValue.getValue();

              outputAggregationsObject = new HashMap<String, DimensionObject<String>>();

              for (Entry<AggregateOperation, Number> operation : operations.entrySet()) {
                AggregateOperation aggrOperationType = operation.getKey();
                Number aggr = operation.getValue();

                String outKey = key + "." + aggrOperationType.name();
                DimensionObject<String> outDimObj = new DimensionObject<String>((MutableDouble)aggr, dimValueName);

                outputAggregationsObject.put(outKey, outDimObj);

              }
              aggregationsOutput.emit(outputAggregationsObject);
            }

            // remove emitted key
            obsoleteKeys.add(key);
          }
        }

        for (String key : obsoleteKeys) {
          cacheObject.remove(key);
        }
      }
    }
  }

  /**
   * supply the registry object which is used to store and retrieve meta information about each tuple
   *
   * @param registry
   */
  public void setRegistry(PropertyRegistry<String> registry)
  {
    this.registry = registry;
  }

  /**
   * Supply the properties to the operator.
   * The properties include type, timebucket, dimensioncombinations, values and operations on them
   * Input includes following properties:
   * type=logtype // input logtype for which the properties are to be set
   * timebucket= time bucket character //time bucket can be one of the following values s(for second)/m(for minute)/h(for hour)/D(for day)/W(for week)/M(for month)/Y(for year)
   * dimensions=a:b:c //colon separated dimension combination combination for which computations are expected
   * values=value.metric[:value.metric] //list of dot concatenated value name and metric separated by colon
   * eg: type=apache,timebucket=m,timebucket=h,dimensions=a:b:c,dimensions=b:c,dimensions=b,dimensions=d,values=x.sum:y.sum:y.avg
   *
   * @param properties
   */
  public void addPropertiesFromString(String[] properties)
  {
    try {
      ArrayList<Integer> dimCombinations = new ArrayList<Integer>();
      HashMap<String, HashSet<AggregateOperation>> valOpTypes = new HashMap<String, HashSet<AggregateOperation>>();
      String type = null;
      // user input example::
      // type=apache,timebucket=m,timebucket=h,dimensions=a:b:c,dimensions=b:c,dimensions=b,dimensions=d,values=x.sum:y.sum:y.avg
      for (String inputs : properties) {
        String[] split = inputs.split("=", 2);
        if (split[0].toLowerCase().equals("timebucket")) {

          int timeBucket = LogstreamUtil.extractTimeBucket(split[1]);
          if (timeBucket == 0) {
            logger.error("invalid time bucket", split[1]);
          }
          timeBucketFlags |= timeBucket;
        }
        else if (split[0].toLowerCase().equals("values")) {
          String[] values = split[1].split(":");
          for (String value : values) {
            String[] valueNames = value.split("\\.");
            String valueName = valueNames[0];
            String valueType = valueNames[1];
            if (valueType.toLowerCase().equals("sum")) {
              if (valOpTypes.containsKey(valueName)) {
                valOpTypes.get(valueName).add(AggregateOperation.SUM);
              }
              else {
                HashSet<AggregateOperation> valueTypeList = new HashSet<AggregateOperation>();
                valueTypeList.add(AggregateOperation.SUM);
                valOpTypes.put(valueName, valueTypeList);
              }
            }
            else if (valueType.equals("avg") || valueType.equals("average")) {
              if (valOpTypes.containsKey(valueName)) {
                valOpTypes.get(valueName).add(AggregateOperation.AVERAGE);
                valOpTypes.get(valueName).add(AggregateOperation.COUNT);
              }
              else {
                HashSet<AggregateOperation> valueTypeList = new HashSet<AggregateOperation>();
                valueTypeList.add(AggregateOperation.AVERAGE);
                valueTypeList.add(AggregateOperation.COUNT);
                valOpTypes.put(valueName, valueTypeList);
              }
            }
            else if (valueType.equals("count")) {
              if (valOpTypes.containsKey(valueName)) {
                valOpTypes.get(valueName).add(AggregateOperation.COUNT);
              }
              else {
                HashSet<AggregateOperation> valueTypeList = new HashSet<AggregateOperation>();
                valueTypeList.add(AggregateOperation.COUNT);
                valOpTypes.put(valueName, valueTypeList);
              }
            }
          }
        }
        else if (split[0].toLowerCase().equals("type")) {
          type = split[1];
        }
        else if (split[0].toLowerCase().equals("dimensions")) {
          // dimensions
          String dimensions = split[1];
          int dim = registry.bind("DIMENSION", dimensions);
          dimCombinations.add(dim);
        }
        else {
          throw new ValidationException("Invalid input property string " + Arrays.toString(properties));
        }
      }

      dimensionCombinationList.put(registry.getIndex(LogstreamUtil.LOG_TYPE, type), dimCombinations);
      valueOperations.put(registry.getIndex(LogstreamUtil.LOG_TYPE, type), valOpTypes);

    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Collection<Partition<DimensionOperator>> definePartitions(Collection<Partition<DimensionOperator>> partitions, int incrementalCapacity)
  {
    ArrayList<Partition<DimensionOperator>> newPartitions = new ArrayList<Partition<DimensionOperator>>();
    String[] filters = registry.list(LogstreamUtil.FILTER);
    int partitionSize;

    if (partitions.size() == 1) {
      // initial partitions; functional partitioning
      partitionSize = filters.length;
    }
    else {
      // redo partitions; double the partitions
      partitionSize = partitions.size() * 2;
    }

    for (int i = 0; i < partitionSize; i++) {
      try {
        DimensionOperator dimensionOperator = new DimensionOperator();
        dimensionOperator.registry = registry;
        dimensionOperator.timeBucketFlags = timeBucketFlags;
        dimensionOperator.valueOperations = new HashMap<Integer, HashMap<String, HashSet<AggregateOperation>>>(valueOperations);
        dimensionOperator.dimensionCombinationList = new HashMap<Integer, ArrayList<Integer>>(dimensionCombinationList);

        Partition<DimensionOperator> partition = new DefaultPartition<DimensionOperator>(dimensionOperator);
        newPartitions.add(partition);
      }
      catch (Throwable ex) {
        DTThrowable.rethrow(ex);
      }
    }

    int partitionBits = (Integer.numberOfLeadingZeros(0) - Integer.numberOfLeadingZeros(partitionSize / filters.length - 1));
    int partitionMask = 0;
    if (partitionBits > 0) {
      partitionMask = -1 >>> (Integer.numberOfLeadingZeros(-1)) - partitionBits;
    }

    partitionMask = (partitionMask << 16) | 0xffff; // right most 16 bits used for functional partitioning

    for (int i = 0; i < newPartitions.size(); i++) {
      Partition<DimensionOperator> partition = newPartitions.get(i);
      String partitionVal = filters[i % filters.length];
      int bits = i / filters.length;
      int filterId = registry.getIndex(LogstreamUtil.FILTER, partitionVal);
      filterId = 0xffff & filterId; // clear out first 16 bits
      int partitionKey = (bits << 16) | filterId; // first 16 bits for dynamic partitioning, last 16 bits for functional partitioning
      logger.debug("partitionKey = {} partitionMask = {}", Integer.toBinaryString(partitionKey), Integer.toBinaryString(partitionMask));
      partition.getPartitionKeys().put(in, new PartitionKeys(partitionMask, Sets.newHashSet(partitionKey)));
    }

    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<DimensionOperator>> partitions)
  {
  }

  /**
   * Time key for dimension computations, if not provided then window timestamp is used
   *
   * @param timeKeyName
   */
  public void setTimeKeyName(String timeKeyName)
  {
    this.timeKeyName = timeKeyName;
  }

  /**
   * extracts the meta information about the tuple
   *
   * @param tuple
   */
  private void extractType(Map<String, Object> tuple)
  {
    recordType.put(LogstreamUtil.LOG_TYPE, (Number)tuple.get(LogstreamUtil.LOG_TYPE));
    recordType.put(LogstreamUtil.FILTER, (Number)tuple.get(LogstreamUtil.FILTER));
  }

  private void createAllDimensionCombinations()
  {
    logger.info("need to create all dimensions for type {}", recordType.get(LogstreamUtil.LOG_TYPE));
    //TODO create all dim combinations
    // temporary code to skip null pointer
    dimensionCombinationList.put((Integer)recordType.get(LogstreamUtil.LOG_TYPE), new ArrayList<Integer>());
  }

  public static class DimensionOperatorStreamCodec extends KryoSerializableStreamCodec<Map<String, Object>>
  {
    private static final Logger logger = LoggerFactory.getLogger(DimensionOperatorStreamCodec.class);

    @Override
    public int getPartition(Map<String, Object> o)
    {
      int ret = 0;

      int filterId = (Integer)o.get(LogstreamUtil.FILTER);
      int hashCode = o.hashCode();

      filterId = 0xffff & filterId; // clear out first 16 bits

      ret = (hashCode << 16) | filterId; // first 16 bits represent hashcode, last 16 bits represent filter type

      return ret;

    }

  }

}
