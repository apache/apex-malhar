/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.schemas;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.query.serde.DataQueryDimensionalDeserializer;
import com.datatorrent.lib.appdata.query.serde.DataQueryDimensionalValidator;
import com.datatorrent.lib.appdata.query.serde.MessageDeserializerInfo;
import com.datatorrent.lib.appdata.query.serde.MessageType;
import com.datatorrent.lib.appdata.query.serde.MessageValidatorInfo;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;

/**
 * This class represents a data query for the {@link DimensionalSchema}.
 * @since 3.1.0
 */
@MessageType(type=DataQueryDimensional.TYPE)
@MessageDeserializerInfo(clazz=DataQueryDimensionalDeserializer.class)
@MessageValidatorInfo(clazz=DataQueryDimensionalValidator.class)
public class DataQueryDimensional extends Query
{
  /**
   * The Message type for this query.
   */
  public static final String TYPE = "dataQuery";

  /**
   * JSON key for time.
   */
  public static final String FIELD_TIME = "time";
  /**
   * JSON key for from.
   */
  public static final String FIELD_FROM = "from";
  /**
   * JSON key for to.
   */
  public static final String FIELD_TO = "to";
  /**
   * JSON key for latestNumBuckets.
   */
  public static final String FIELD_LATEST_NUM_BUCKETS = "latestNumBuckets";
  /**
   * JSON key for bucket.
   */
  public static final String FIELD_BUCKET = "bucket";
  /**
   * JSON key for fields.
   */
  public static final String FIELD_FIELDS = DataQuerySnapshot.FIELD_FIELDS;
  /**
   * JSON key for data.
   */
  public static final String FIELD_DATA = DataQuerySnapshot.FIELD_DATA;
  /**
   * JSON key for keys.
   */
  public static final String FIELD_KEYS = "keys";
  /**
   * JSON key for incompleteResultOK.
   */
  public static final String FIELD_INCOMPLETE_RESULT_OK = "incompleteResultOK";
  /**
   * JSON key for sliding aggregates.
   */
  public static final String FIELD_SLIDING_AGGREGATE_SIZE = "slidingAggregateSize";
  /**
   * The from timestamp.
   */
  private long from;
  /**
   * The to timestamp.
   */
  private long to;
  /**
   * The latestNumber of buckets to retrieve.
   */
  private int latestNumBuckets = -1;
  /**
   * The timeBucket to retrieve data from.
   */
  private TimeBucket timeBucket;
  private CustomTimeBucket customTimeBucket;
  /**
   * The dimensional keys.
   */
  @Deprecated
  private GPOMutable keys;
  /**
   * Whether incomplete results are OK or not.
   */
  private boolean incompleteResultOK = true;
  /**
   * Whether the query has a time component or not.
   */
  private boolean hasTime = false;
  /**
   * Whether the query has a from and to time or not.
   */
  private boolean fromTo = false;
  /**
   * The set of fields that are keys in this query.
   */
  private Fields keyFields;
  /**
   * The dimensions descriptor / dimension combination for this query.
   */
  private DimensionsDescriptor dimensionsDescriptor;
  private FieldsAggregatable fieldsAggregatable;
  private int slidingAggregateSize = 1;

  private FieldsDescriptor fd;
  private Map<String, Set<Object>> keysToQueryValues;

  public DataQueryDimensional(String id,
                              String type,
                              FieldsDescriptor fd,
                              Map<String, Set<Object>> keysToQueryValues,
                              FieldsAggregatable fieldsAggregatable,
                              boolean incompleteResultOK)
  {
    this(id,
         type,
         fd,
         keysToQueryValues,
         fieldsAggregatable,
         incompleteResultOK,
         null);
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              boolean incompleteResultOK)
  {
    this(id,
         type,
         keys,
         fieldsAggregatable,
         incompleteResultOK,
         null);
  }

  public DataQueryDimensional(String id,
                              String type,
                              FieldsDescriptor fd,
                              Map<String, Set<Object>> keysToQueryValues,
                              FieldsAggregatable fieldsAggregatable,
                              boolean incompleteResultOK,
                              Map<String, String> schemaKeys)
  {
    super(id, type, schemaKeys);
    setFd(fd);
    setKeysToQueryValues(keysToQueryValues);
    setFieldsAggregatable(fieldsAggregatable);
    setIncompleteResultOK(incompleteResultOK);
    this.hasTime = false;

    initialize();
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              boolean incompleteResultOK,
                              Map<String, String> schemaKeys)
  {
    super(id, type, schemaKeys);
    setKeys(keys);
    setFieldsAggregatable(fieldsAggregatable);
    setIncompleteResultOK(incompleteResultOK);
    this.hasTime = false;

    initialize();
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              int latestNumBuckets,
                              TimeBucket timeBucket,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              boolean incompleteResultOK)
  {
    this(id,
         type,
         latestNumBuckets,
         new CustomTimeBucket(timeBucket),
         keys,
         fieldsAggregatable,
         incompleteResultOK,
         null);
  }

  public DataQueryDimensional(String id,
                              String type,
                              int latestNumBuckets,
                              CustomTimeBucket timeBucket,
                              FieldsDescriptor fd,
                              Map<String, Set<Object>> keysToQueryValues,
                              FieldsAggregatable fieldsAggregatable,
                              boolean incompleteResultOK)
  {
    this(id,
         type,
         latestNumBuckets,
         timeBucket,
         fd,
         keysToQueryValues,
         fieldsAggregatable,
         incompleteResultOK,
         null);
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              int latestNumBuckets,
                              CustomTimeBucket timeBucket,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              boolean incompleteResultOK)
  {
    this(id,
         type,
         latestNumBuckets,
         timeBucket,
         keys,
         fieldsAggregatable,
         incompleteResultOK,
         null);
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              int latestNumBuckets,
                              TimeBucket timeBucket,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              boolean incompleteResultOK,
                              Map<String, String> schemaKeys)
  {
    this(id,
         type,
         latestNumBuckets,
         new CustomTimeBucket(timeBucket),
         keys,
         fieldsAggregatable,
         incompleteResultOK,
         schemaKeys);
  }

  public DataQueryDimensional(String id,
                              String type,
                              int latestNumBuckets,
                              CustomTimeBucket timeBucket,
                              FieldsDescriptor fd,
                              Map<String, Set<Object>> keysToQueryValues,
                              FieldsAggregatable fieldsAggregatable,
                              boolean incompleteResultOK,
                              Map<String, String> schemaKeys)
  {
    super(id, type, schemaKeys);
    setLatestNumBuckets(latestNumBuckets);
    setCustomTimeBucket(timeBucket);
    setFd(fd);
    setKeysToQueryValues(keysToQueryValues);
    setFieldsAggregatable(fieldsAggregatable);
    setIncompleteResultOK(incompleteResultOK);
    this.fromTo = false;
    this.hasTime = true;

    initialize();
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              int latestNumBuckets,
                              CustomTimeBucket timeBucket,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              boolean incompleteResultOK,
                              Map<String, String> schemaKeys)
  {
    super(id, type, schemaKeys);
    setLatestNumBuckets(latestNumBuckets);
    setCustomTimeBucket(timeBucket);
    setKeys(keys);
    setFieldsAggregatable(fieldsAggregatable);
    setIncompleteResultOK(incompleteResultOK);
    this.fromTo = false;
    this.hasTime = true;

    initialize();
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              long from,
                              long to,
                              TimeBucket timeBucket,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              boolean incompleteResultOK)
  {
    this(id,
         type,
         from,
         to,
         new CustomTimeBucket(timeBucket),
         keys,
         fieldsAggregatable,
         incompleteResultOK);
  }

  public DataQueryDimensional(String id,
                              String type,
                              long from,
                              long to,
                              CustomTimeBucket timeBucket,
                              FieldsDescriptor fd,
                              Map<String, Set<Object>> keysToQueryValues,
                              FieldsAggregatable fieldsAggregatable,
                              boolean incompleteResultOK)
  {
    this(id,
         type,
         from,
         to,
         timeBucket,
         fd,
         keysToQueryValues,
         fieldsAggregatable,
         incompleteResultOK,
         null);
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              long from,
                              long to,
                              CustomTimeBucket timeBucket,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              boolean incompleteResultOK)
  {
    this(id,
         type,
         from,
         to,
         timeBucket,
         keys,
         fieldsAggregatable,
         incompleteResultOK,
         null);
  }

  @Deprecated
  public DataQueryDimensional(String id,
                       String type,
                       long from,
                       long to,
                       TimeBucket timeBucket,
                       GPOMutable keys,
                       FieldsAggregatable fieldsAggregatable,
                       boolean incompleteResultOK,
                       Map<String, String> schemaKeys)
  {
    this(id,
         type,
         from,
         to,
         new CustomTimeBucket(timeBucket),
         keys,
         fieldsAggregatable,
         incompleteResultOK,
         schemaKeys);
  }

  public DataQueryDimensional(String id,
                              String type,
                              long from,
                              long to,
                              CustomTimeBucket timeBucket,
                              FieldsDescriptor fd,
                              Map<String, Set<Object>> keysToQueryValues,
                              FieldsAggregatable fieldsAggregatable,
                              boolean incompleteResultOK,
                              Map<String, String> schemaKeys)
  {
    super(id, type, schemaKeys);
    setFrom(from);
    setTo(to);
    setCustomTimeBucket(timeBucket);
    setFd(fd);
    setKeysToQueryValues(keysToQueryValues);
    setFieldsAggregatable(fieldsAggregatable);
    setIncompleteResultOK(incompleteResultOK);
    this.fromTo = true;
    this.hasTime = true;

    initialize();
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              long from,
                              long to,
                              CustomTimeBucket timeBucket,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              boolean incompleteResultOK,
                              Map<String, String> schemaKeys)
  {
    super(id, type, schemaKeys);
    setFrom(from);
    setTo(to);
    setCustomTimeBucket(timeBucket);
    setKeys(keys);
    setFieldsAggregatable(fieldsAggregatable);
    setIncompleteResultOK(incompleteResultOK);
    this.fromTo = true;
    this.hasTime = true;

    initialize();
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              long from,
                              long to,
                              TimeBucket timeBucket,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              long countdown,
                              boolean incompleteResultOK)
  {
    this(id,
         type,
         from,
         to,
         new CustomTimeBucket(timeBucket),
         keys,
         fieldsAggregatable,
         countdown,
         incompleteResultOK);
  }

  public DataQueryDimensional(String id,
                              String type,
                              long from,
                              long to,
                              CustomTimeBucket timeBucket,
                              FieldsDescriptor fd,
                              Map<String, Set<Object>> keysToQueryValues,
                              FieldsAggregatable fieldsAggregatable,
                              long countdown,
                              boolean incompleteResultOK)
  {
    this(id,
         type,
         from,
         to,
         timeBucket,
         fd,
         keysToQueryValues,
         fieldsAggregatable,
         countdown,
         incompleteResultOK,
         null);
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              long from,
                              long to,
                              CustomTimeBucket timeBucket,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              long countdown,
                              boolean incompleteResultOK)
  {
    this(id,
         type,
         from,
         to,
         timeBucket,
         keys,
         fieldsAggregatable,
         countdown,
         incompleteResultOK,
         null);
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              long from,
                              long to,
                              TimeBucket timeBucket,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              long countdown,
                              boolean incompleteResultOK,
                              Map<String, String> schemaKeys)
  {
    this(id,
         type,
         from,
         to,
         new CustomTimeBucket(timeBucket),
         keys,
         fieldsAggregatable,
         countdown,
         incompleteResultOK,
         schemaKeys);
  }

  public DataQueryDimensional(String id,
                              String type,
                              long from,
                              long to,
                              CustomTimeBucket timeBucket,
                              FieldsDescriptor fd,
                              Map<String, Set<Object>> keysToQueryValues,
                              FieldsAggregatable fieldsAggregatable,
                              long countdown,
                              boolean incompleteResultOK,
                              Map<String, String> schemaKeys)
  {
    super(id, type, countdown, schemaKeys);
    setFrom(from);
    setTo(to);
    setCustomTimeBucket(timeBucket);
    setFd(fd);
    setKeysToQueryValues(keysToQueryValues);
    setFieldsAggregatable(fieldsAggregatable);
    setIncompleteResultOK(incompleteResultOK);
    this.fromTo = true;
    this.hasTime = true;

    initialize();
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              long from,
                              long to,
                              CustomTimeBucket timeBucket,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              long countdown,
                              boolean incompleteResultOK,
                              Map<String, String> schemaKeys)
  {
    super(id, type, countdown, schemaKeys);
    setFrom(from);
    setTo(to);
    setCustomTimeBucket(timeBucket);
    setKeys(keys);
    setFieldsAggregatable(fieldsAggregatable);
    setIncompleteResultOK(incompleteResultOK);
    this.fromTo = true;
    this.hasTime = true;

    initialize();
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              int latestNumBuckets,
                              TimeBucket timeBucket,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              long countdown,
                              boolean incompleteResultOK)
  {
    this(id,
         type,
         latestNumBuckets,
         new CustomTimeBucket(timeBucket),
         keys,
         fieldsAggregatable,
         countdown,
         incompleteResultOK);
  }

  public DataQueryDimensional(String id,
                              String type,
                              int latestNumBuckets,
                              CustomTimeBucket timeBucket,
                              FieldsDescriptor fd,
                              Map<String, Set<Object>> keysToQueryValues,
                              FieldsAggregatable fieldsAggregatable,
                              long countdown,
                              boolean incompleteResultOK)
  {
    super(id, type, countdown);
    setLatestNumBuckets(latestNumBuckets);
    setCustomTimeBucket(timeBucket);
    setFd(fd);
    setKeysToQueryValues(keysToQueryValues);
    setFieldsAggregatable(fieldsAggregatable);
    setIncompleteResultOK(incompleteResultOK);
    this.fromTo = false;
    this.hasTime = true;

    initialize();
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              int latestNumBuckets,
                              CustomTimeBucket timeBucket,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              long countdown,
                              boolean incompleteResultOK)
  {
    super(id, type, countdown);
    setLatestNumBuckets(latestNumBuckets);
    setCustomTimeBucket(timeBucket);
    setKeys(keys);
    setFieldsAggregatable(fieldsAggregatable);
    setIncompleteResultOK(incompleteResultOK);
    this.fromTo = false;
    this.hasTime = true;

    initialize();
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              int latestNumBuckets,
                              TimeBucket timeBucket,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              long countdown,
                              boolean incompleteResultOK,
                              Map<String, String> schemaKeys)
  {
    this(id,
         type,
         latestNumBuckets,
         new CustomTimeBucket(timeBucket),
         keys,
         fieldsAggregatable,
         countdown,
         incompleteResultOK,
         schemaKeys);
  }

  public DataQueryDimensional(String id,
                              String type,
                              int latestNumBuckets,
                              CustomTimeBucket timeBucket,
                              FieldsDescriptor fd,
                              Map<String, Set<Object>> keysToQueryValues,
                              FieldsAggregatable fieldsAggregatable,
                              long countdown,
                              boolean incompleteResultOK,
                              Map<String, String> schemaKeys)
  {
    super(id, type, countdown, schemaKeys);
    setLatestNumBuckets(latestNumBuckets);
    setCustomTimeBucket(timeBucket);
    setFd(fd);
    setKeysToQueryValues(keysToQueryValues);
    setFieldsAggregatable(fieldsAggregatable);
    setIncompleteResultOK(incompleteResultOK);
    this.fromTo = false;
    this.hasTime = true;

    initialize();
  }

  @Deprecated
  public DataQueryDimensional(String id,
                              String type,
                              int latestNumBuckets,
                              CustomTimeBucket timeBucket,
                              GPOMutable keys,
                              FieldsAggregatable fieldsAggregatable,
                              long countdown,
                              boolean incompleteResultOK,
                              Map<String, String> schemaKeys)
  {
    super(id, type, countdown, schemaKeys);
    setLatestNumBuckets(latestNumBuckets);
    setCustomTimeBucket(timeBucket);
    setKeys(keys);
    setFieldsAggregatable(fieldsAggregatable);
    setIncompleteResultOK(incompleteResultOK);
    this.fromTo = false;
    this.hasTime = true;

    initialize();
  }

  private void initialize()
  {
    Set<String> keyFieldSet = Sets.newHashSet();

    if (keys != null) {
      keyFieldSet.addAll(keys.getFieldDescriptor().getFields().getFields());

      keysToQueryValues = Maps.newHashMap();

      for (String key : keyFieldSet) {
        Set<Object> values = Sets.newHashSet();
        values.add(keys.getField(key));
        keysToQueryValues.put(key, values);
      }

    } else {
      keyFieldSet.addAll(this.fd.getFields().getFields());
    }

    keyFields = new Fields(keyFieldSet);

    if(!hasTime) {
      timeBucket = TimeBucket.ALL;
      customTimeBucket = new CustomTimeBucket(TimeBucket.ALL);
    }

    dimensionsDescriptor = new DimensionsDescriptor(customTimeBucket, keyFields);
  }

  public Fields getKeyFields()
  {
    return keyFields;
  }

  @Deprecated
  public GPOMutable createKeyGPO(FieldsDescriptor fd)
  {
    GPOMutable gpo = new GPOMutable(fd);

    for(String field: gpo.getFieldDescriptor().getFields().getFields()) {
      if(hasTime) {
        if(field.equals(DimensionsDescriptor.DIMENSION_TIME)) {
          continue;
        }
        else if(field.equals(DimensionsDescriptor.DIMENSION_TIME_BUCKET)) {
          gpo.setField(field, this.timeBucket.ordinal());
        }
      }

      if(DimensionsDescriptor.RESERVED_DIMENSION_NAMES.contains(field)) {
        continue;
      }

      gpo.setFieldGeneric(field, keys.getField(field));
    }

    return gpo;
  }

  private void setFieldsAggregatable(FieldsAggregatable fieldsAggregatable)
  {
    this.fieldsAggregatable = Preconditions.checkNotNull(fieldsAggregatable, "fieldsAggregatable");
  }

  private void setIncompleteResultOK(boolean incompleteResultOK)
  {
    this.incompleteResultOK = incompleteResultOK;
  }

  public boolean getIncompleteResultOK()
  {
    return incompleteResultOK;
  }

  private void setFrom(long from)
  {
    this.from = from;
  }

  public long getFrom()
  {
    return from;
  }

  private void setTo(long to)
  {
    this.to = to;
  }

  public long getTo()
  {
    return to;
  }

  private void setCustomTimeBucket(CustomTimeBucket customTimeBucket)
  {
    this.customTimeBucket = Preconditions.checkNotNull(customTimeBucket);
    this.timeBucket = customTimeBucket.getTimeBucket();
  }

  public TimeBucket getTimeBucket()
  {
    return timeBucket;
  }

  public CustomTimeBucket getCustomTimeBucket()
  {
    return customTimeBucket;
  }

  private void setKeys(GPOMutable keys)
  {
    this.keys = Preconditions.checkNotNull(keys);
  }

  @Deprecated
  public GPOMutable getKeys()
  {
    return keys;
  }

  /**
   * @return the latestNumBuckets
   */
  public int getLatestNumBuckets()
  {
    return latestNumBuckets;
  }

  /**
   * @param latestNumBuckets the latestNumBuckets to set
   */
  private void setLatestNumBuckets(int latestNumBuckets)
  {
    this.latestNumBuckets = latestNumBuckets;
  }

  /**
   * @return the dimensionsDescriptor
   */
  public DimensionsDescriptor getDimensionsDescriptor()
  {
    return dimensionsDescriptor;
  }

  /**
   * @return the fromTo
   */
  public boolean isFromTo()
  {
    return fromTo;
  }

  /**
   * @return the hasTime
   */
  public boolean isHasTime()
  {
    return hasTime;
  }

  /**
   * @return the fieldsAggregatable
   */
  public FieldsAggregatable getFieldsAggregatable()
  {
    return fieldsAggregatable;
  }

  /**
   * @return the slidingAggregateSize
   */
  public int getSlidingAggregateSize()
  {
    return slidingAggregateSize;
  }

  /**
   * @param slidingAggregateSize the slidingAggregateSize to set
   */
  public void setSlidingAggregateSize(int slidingAggregateSize)
  {
    Preconditions.checkArgument(slidingAggregateSize >= 1,
                                "The slidingAggregateSize must be greater than or equal to one.");
    this.slidingAggregateSize = slidingAggregateSize;
  }

  /**
   * @return the fd
   */
  public FieldsDescriptor getFd()
  {
    return fd;
  }

  /**
   * @param fd the fd to set
   */
  private void setFd(FieldsDescriptor fd)
  {
    this.fd = Preconditions.checkNotNull(fd);
  }

  /**
   * @return the keysToQueryValues
   */
  public Map<String, Set<Object>> getKeysToQueryValues()
  {
    return keysToQueryValues;
  }

  /**
   * @param keysToQueryValues the keysToQueryValues to set
   */
  private void setKeysToQueryValues(Map<String, Set<Object>> keysToQueryValues)
  {
    for (Map.Entry<String, Set<Object>> entry : keysToQueryValues.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());

      for (Object obj : entry.getValue()) {
        Preconditions.checkNotNull(obj);
      }
    }

    this.keysToQueryValues = keysToQueryValues;
  }

  @Override
  public int hashCode()
  {
    int hash = 3;
    hash = 59 * hash + ((int) from);
    hash = 59 * hash + ((int) to);
    hash = 59 * hash + this.latestNumBuckets;
    hash = 59 * hash + (this.customTimeBucket != null ? this.customTimeBucket.hashCode() : 0);
    hash = 59 * hash + (this.keys != null ? this.keys.hashCode() : 0);
    hash = 59 * hash + (this.incompleteResultOK ? 1 : 0);
    hash = 59 * hash + (this.hasTime ? 1 : 0);
    hash = 59 * hash + (this.fromTo ? 1 : 0);
    hash = 59 * hash + (this.dimensionsDescriptor != null ? this.dimensionsDescriptor.hashCode() : 0);
    hash = 59 * hash + (this.fieldsAggregatable != null ? this.fieldsAggregatable.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    if(obj == null) {
      return false;
    }
    if(getClass() != obj.getClass()) {
      return false;
    }
    final DataQueryDimensional other = (DataQueryDimensional)obj;
    if(this.from != other.from) {
      return false;
    }
    if(this.to != other.to) {
      return false;
    }
    if(this.latestNumBuckets != other.latestNumBuckets) {
      return false;
    }
    if(this.customTimeBucket.equals(other.customTimeBucket)) {
      return false;
    }
    if(this.keys != other.keys && (this.keys == null || !this.keys.equals(other.keys))) {
      return false;
    }
    if(this.incompleteResultOK != other.incompleteResultOK) {
      return false;
    }
    if(this.hasTime != other.hasTime) {
      return false;
    }
    if(this.fromTo != other.fromTo) {
      return false;
    }
    if(this.dimensionsDescriptor != other.dimensionsDescriptor && (this.dimensionsDescriptor == null || !this.dimensionsDescriptor.equals(other.dimensionsDescriptor))) {
      return false;
    }
    if(this.fieldsAggregatable != other.fieldsAggregatable && (this.fieldsAggregatable == null || !this.fieldsAggregatable.equals(other.fieldsAggregatable))) {
      return false;
    }
    return true;
  }

  @Override
  public String toString()
  {
    return "GenericDataQuery{" + "from=" + from + ", to=" + to + ", latestNumBuckets=" + latestNumBuckets + ", timeBucket=" + timeBucket + ", countdown=" + getCountdown() + ", incompleteResultOK=" + incompleteResultOK + ", hasTime=" + hasTime + ", oneTime=" + isOneTime() + ", fromTo=" + fromTo + '}';
  }
}
