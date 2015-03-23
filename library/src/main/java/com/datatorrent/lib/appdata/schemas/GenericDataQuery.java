/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.qr.DataDeserializerInfo;
import com.datatorrent.lib.appdata.qr.DataType;
import com.datatorrent.lib.appdata.qr.DataValidatorInfo;
import com.datatorrent.lib.appdata.qr.Query;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

@DataType(type=GenericDataQuery.TYPE)
@DataDeserializerInfo(clazz=GenericDataQueryDeserializer.class)
@DataValidatorInfo(clazz=GenericDataQueryValidator.class)
public class GenericDataQuery extends Query
{
  public static final String TYPE = "dataQuery";

  public static final String FIELD_DATA = "data";
  public static final String FIELD_TIME = "time";
  public static final String FIELD_FROM = "from";
  public static final String FIELD_TO = "to";
  public static final String FIELD_LATEST_NUM_BUCKETS = "latestNumBuckets";
  public static final String FIELD_BUCKET = "bucket";

  public static final String FIELD_KEYS = "keys";
  public static final String FIELD_FIELDS = "fields";
  public static final String FIELD_COUNTDOWN = "countdown";
  public static final String FIELD_INCOMPLETE_RESULT_OK = "incompleteResultOK";

  public static final FieldsDescriptor TIME_FIELD_DESCRIPTOR;

  static
  {
    Map<String, Type> fieldToType = Maps.newHashMap();

    fieldToType.put(DimensionsDescriptor.DIMENSION_TIME, DimensionsDescriptor.DIMENSION_TIME_TYPE);
    fieldToType.put(DimensionsDescriptor.DIMENSION_TIME_BUCKET, DimensionsDescriptor.DIMENSION_TIME_BUCKET_TYPE);

    TIME_FIELD_DESCRIPTOR = new FieldsDescriptor(fieldToType);
  }

  private String from;
  private String to;
  private int latestNumBuckets = -1;
  private TimeBucket timeBucket;
  private GPOMutable keys;
  //Value fields selected in query.
  private Fields fields;
  private long countdown;
  private boolean incompleteResultOK = true;
  private boolean hasTime = false;
  private boolean oneTime = false;
  private boolean fromTo = false;
  private Fields keyFields;

  public GenericDataQuery(GPOImmutable keys,
                          Fields fields,
                          boolean incompleteResultOK)
  {
    setKeys(keys);
    setFields(fields);
    setIncompleteResultOK(incompleteResultOK);
    this.hasTime = false;

    initialize();
  }

  public GenericDataQuery(int latestNumBuckets,
                          TimeBucket timeBucket,
                          GPOImmutable keys,
                          Fields fields,
                          boolean incompleteResultOK)
  {
    setLatestNumBuckets(latestNumBuckets);
    setTimeBucket(timeBucket);
    setKeys(keys);
    setFields(fields);
    setIncompleteResultOK(incompleteResultOK);
    this.oneTime = true;
    this.fromTo = false;
    this.hasTime = true;

    initialize();
  }

  public GenericDataQuery(String from,
                          String to,
                          TimeBucket timeBucket,
                          GPOImmutable keys,
                          Fields fields,
                          boolean incompleteResultOK)
  {
    setFrom(from);
    setTo(to);
    setTimeBucket(timeBucket);
    setKeys(keys);
    setFields(fields);
    setIncompleteResultOK(incompleteResultOK);
    this.oneTime = true;
    this.fromTo = true;
    this.hasTime = true;

    initialize();
  }

  public GenericDataQuery(String from,
                          String to,
                          TimeBucket timeBucket,
                          GPOImmutable keys,
                          Fields fields,
                          long countdown,
                          boolean incompleteResultOK)
  {
    setFrom(from);
    setTo(to);
    setTimeBucket(timeBucket);
    setKeys(keys);
    setFields(fields);
    setCountdown(countdown);
    setIncompleteResultOK(incompleteResultOK);
    this.oneTime = false;
    this.fromTo = true;
    this.hasTime = true;

    initialize();
  }

  public GenericDataQuery(int latestNumBuckets,
                          TimeBucket timeBucket,
                          GPOImmutable keys,
                          Fields fields,
                          long countdown,
                          boolean incompleteResultOK)
  {
    setLatestNumBuckets(latestNumBuckets);
    setTimeBucket(timeBucket);
    setKeys(keys);
    setFields(fields);
    setCountdown(countdown);
    setIncompleteResultOK(incompleteResultOK);
    this.oneTime = false;
    this.fromTo = false;
    this.hasTime = true;

    initialize();
  }

  private void initialize()
  {
    Set<String> keyFieldSet = Sets.newHashSet();
    keyFieldSet.addAll(keys.getFieldDescriptor().getFields().getFields());

    if(hasTime) {
      keyFieldSet.add(DimensionsDescriptor.DIMENSION_TIME);
      keyFieldSet.add(DimensionsDescriptor.DIMENSION_TIME_BUCKET);
    }

    keyFields = new Fields(keyFieldSet);
  }

  public Fields getKeyFields()
  {
    return keyFields;
  }

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
      else {
        gpo.setField(field, keys.getField(field));
      }
    }

    return gpo;
  }

  private void setIncompleteResultOK(boolean incompleteResultOK)
  {
    this.incompleteResultOK = incompleteResultOK;
  }

  public boolean getIncompleteResultOK()
  {
    return incompleteResultOK;
  }

  private void setCountdown(long countdown)
  {
    Preconditions.checkArgument(countdown > 0, "Countdown must be positive.");
    this.countdown = countdown;
  }

  public long getCountdown()
  {
    return countdown;
  }

  private void setFrom(String from)
  {
    Preconditions.checkNotNull(from);
    SchemaUtils.checkDateEx(from);
    this.from = from;
  }

  public String getFrom()
  {
    return from;
  }

  public long getFromLong()
  {
    return SchemaUtils.getLong(from);
  }

  private void setTo(String to)
  {
    Preconditions.checkNotNull(to);
    SchemaUtils.checkDateEx(to);
    this.to = to;
  }

  public String getTo()
  {
    return to;
  }

  public long getToLong()
  {
    return SchemaUtils.getLong(to);
  }

  private void setTimeBucket(TimeBucket timeBucket)
  {
    Preconditions.checkNotNull(timeBucket);
    this.timeBucket = timeBucket;
  }

  public TimeBucket getTimeBucket()
  {
    return timeBucket;
  }

  private void setKeys(GPOMutable keys)
  {
    Preconditions.checkNotNull(keys);
  }

  public GPOMutable getKeys()
  {
    return keys;
  }

  /**
   * @param fields the fields to set
   */
  private void setFields(Fields fields)
  {
    Preconditions.checkNotNull(fields);
    this.fields = fields;
  }

  /**
   * @return the fields
   */
  public Fields getFields()
  {
    return fields;
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

  public boolean getOneTime()
  {
    return oneTime;
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
}
