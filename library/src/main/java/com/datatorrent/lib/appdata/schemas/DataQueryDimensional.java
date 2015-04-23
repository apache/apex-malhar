/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
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

@DataType(type=DataQueryDimensional.TYPE)
@DataDeserializerInfo(clazz=DataQueryDimensionalDeserializer.class)
@DataValidatorInfo(clazz=DataQueryDimensionalValidator.class)
public class DataQueryDimensional extends Query
{
  public static final String TYPE = "dataQuery";

  public static final String FIELD_TIME = "time";
  public static final String FIELD_FROM = "from";
  public static final String FIELD_TO = "to";
  public static final String FIELD_LATEST_NUM_BUCKETS = "latestNumBuckets";
  public static final String FIELD_BUCKET = "bucket";

  public static final String FIELD_FIELDS = DataQueryTabular.FIELD_FIELDS;
  public static final String FIELD_DATA = DataQueryTabular.FIELD_DATA;

  public static final String FIELD_KEYS = "keys";
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
  private boolean incompleteResultOK = true;
  private boolean hasTime = false;
  private boolean fromTo = false;
  private Fields keyFields;
  private DimensionsDescriptor dd;
  private FieldsAggregatable fieldsAggregatable;

  public DataQueryDimensional(String id,
                          String type,
                          GPOMutable keys,
                          FieldsAggregatable fieldsAggregatable,
                          boolean incompleteResultOK)
  {
    super(id, type);
    setKeys(keys);
    setFieldsAggregatable(fieldsAggregatable);
    setIncompleteResultOK(incompleteResultOK);
    this.hasTime = false;

    initialize();
  }

  public DataQueryDimensional(String id,
                          String type,
                          int latestNumBuckets,
                          TimeBucket timeBucket,
                          GPOMutable keys,
                          FieldsAggregatable fieldsAggregatable,
                          boolean incompleteResultOK)
  {
    super(id, type);
    setLatestNumBuckets(latestNumBuckets);
    setTimeBucket(timeBucket);
    setKeys(keys);
    setFieldsAggregatable(fieldsAggregatable);
    setIncompleteResultOK(incompleteResultOK);
    this.fromTo = false;
    this.hasTime = true;

    initialize();
  }

  public DataQueryDimensional(String id,
                          String type,
                          String from,
                          String to,
                          TimeBucket timeBucket,
                          GPOMutable keys,
                          FieldsAggregatable fieldsAggregatable,
                          boolean incompleteResultOK)
  {
    super(id, type);
    setFrom(from);
    setTo(to);
    setTimeBucket(timeBucket);
    setKeys(keys);
    setFieldsAggregatable(fieldsAggregatable);
    setIncompleteResultOK(incompleteResultOK);
    this.fromTo = true;
    this.hasTime = true;

    initialize();
  }

  public DataQueryDimensional(String id,
                          String type,
                          String from,
                          String to,
                          TimeBucket timeBucket,
                          GPOMutable keys,
                          FieldsAggregatable fieldsAggregatable,
                          long countdown,
                          boolean incompleteResultOK)
  {
    super(id, type, countdown);
    setFrom(from);
    setTo(to);
    setTimeBucket(timeBucket);
    setKeys(keys);
    setFieldsAggregatable(fieldsAggregatable);
    setIncompleteResultOK(incompleteResultOK);
    this.fromTo = true;
    this.hasTime = true;

    initialize();
  }

  public DataQueryDimensional(String id,
                          String type,
                          int latestNumBuckets,
                          TimeBucket timeBucket,
                          GPOMutable keys,
                          FieldsAggregatable fieldsAggregatable,
                          long countdown,
                          boolean incompleteResultOK)
  {
    super(id, type, countdown);
    setLatestNumBuckets(latestNumBuckets);
    setTimeBucket(timeBucket);
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
    keyFieldSet.addAll(keys.getFieldDescriptor().getFields().getFields());

    keyFields = new Fields(keyFieldSet);
    dd = new DimensionsDescriptor(timeBucket,
                                  keyFields);
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

      if(DimensionsDescriptor.RESERVED_DIMENSION_NAMES.contains(field)) {
        continue;
      }

      gpo.setField(field, keys.getField(field));
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
    this.keys = keys;
  }

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
   * @return the dd
   */
  public DimensionsDescriptor getDd()
  {
    return dd;
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

  @Override
  public int hashCode()
  {
    int hash = 3;
    hash = 59 * hash + (this.from != null ? this.from.hashCode() : 0);
    hash = 59 * hash + (this.to != null ? this.to.hashCode() : 0);
    hash = 59 * hash + this.latestNumBuckets;
    hash = 59 * hash + (this.timeBucket != null ? this.timeBucket.hashCode() : 0);
    hash = 59 * hash + (this.keys != null ? this.keys.hashCode() : 0);
    hash = 59 * hash + (this.incompleteResultOK ? 1 : 0);
    hash = 59 * hash + (this.hasTime ? 1 : 0);
    hash = 59 * hash + (this.fromTo ? 1 : 0);
    hash = 59 * hash + (this.dd != null ? this.dd.hashCode() : 0);
    hash = 59 * hash + (this.fieldsAggregatable != null ? this.fieldsAggregatable.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean queueEquals(Query query)
  {
    if(query == null) {
      return false;
    }
    if(getClass() != query.getClass()) {
      return false;
    }
    final DataQueryDimensional other = (DataQueryDimensional) query;
    if((this.from == null) ? (other.from != null) : !this.from.equals(other.from)) {
      return false;
    }
    if((this.to == null) ? (other.to != null) : !this.to.equals(other.to)) {
      return false;
    }
    if(this.timeBucket != other.timeBucket) {
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
    if(this.dd != other.dd && (this.dd == null || !this.dd.equals(other.dd))) {
      return false;
    }
    if(this.fieldsAggregatable != other.fieldsAggregatable && (this.fieldsAggregatable == null || !this.fieldsAggregatable.equals(other.fieldsAggregatable))) {
      return false;
    }
    return true;
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
    if((this.from == null) ? (other.from != null) : !this.from.equals(other.from)) {
      return false;
    }
    if((this.to == null) ? (other.to != null) : !this.to.equals(other.to)) {
      return false;
    }
    if(this.latestNumBuckets != other.latestNumBuckets) {
      return false;
    }
    if(this.timeBucket != other.timeBucket) {
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
    if(this.dd != other.dd && (this.dd == null || !this.dd.equals(other.dd))) {
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
