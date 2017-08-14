/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.dimensions.aggregator;

import java.util.List;
import java.util.Map;

import org.apache.apex.malhar.lib.appdata.gpo.GPOMutable;
import org.apache.apex.malhar.lib.appdata.gpo.GPOUtils;
import org.apache.apex.malhar.lib.appdata.gpo.Serde;
import org.apache.apex.malhar.lib.appdata.gpo.SerdeFieldsDescriptor;
import org.apache.apex.malhar.lib.appdata.gpo.SerdeListGPOMutable;
import org.apache.apex.malhar.lib.appdata.gpo.SerdeObjectPayloadFix;
import org.apache.apex.malhar.lib.appdata.schemas.FieldsDescriptor;
import org.apache.apex.malhar.lib.appdata.schemas.Type;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.Aggregate;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.InputEvent;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.annotation.Name;

@Name("CUM_SUM")
/**
 * @since 3.1.0
 */

public class AggregatorCumSum extends AggregatorSum
{
  private static final long serialVersionUID = 201506280518L;

  public static final int KEY_FD_INDEX = 0;
  public static final int AGGREGATE_FD_INDEX = 1;
  public static final int KEYS_INDEX = 2;
  public static final int AGGREGATES_INDEX = 3;

  public static final FieldsDescriptor META_DATA_FIELDS_DESCRIPTOR;

  static {
    Map<String, Type> fieldToType = Maps.newHashMap();
    fieldToType.put("fdkeys", Type.OBJECT);
    fieldToType.put("fdvalues", Type.OBJECT);
    fieldToType.put("keys", Type.OBJECT);
    fieldToType.put("values", Type.OBJECT);

    Map<String, Serde> fieldToSerde = Maps.newHashMap();
    fieldToSerde.put("fdkeys", SerdeFieldsDescriptor.INSTANCE);
    fieldToSerde.put("fdvalues", SerdeFieldsDescriptor.INSTANCE);
    fieldToSerde.put("keys", SerdeListGPOMutable.INSTANCE);
    fieldToSerde.put("values", SerdeListGPOMutable.INSTANCE);

    META_DATA_FIELDS_DESCRIPTOR = new FieldsDescriptor(fieldToType,
        fieldToSerde,
        new PayloadFix());
  }

  public AggregatorCumSum()
  {
  }

  @Override
  public Aggregate getGroup(InputEvent src, int aggregatorIndex)
  {
    src.used = true;
    Aggregate agg = createAggregate(src,
        context,
        aggregatorIndex);

    GPOUtils.indirectCopy(agg.getAggregates(), src.getAggregates(), context.indexSubsetAggregates);

    GPOMutable metaData = new GPOMutable(getMetaDataDescriptor());

    GPOMutable fullKey = new GPOMutable(src.getKeys());

    if (context.inputTimestampIndex >= 0) {
      fullKey.getFieldsLong()[context.inputTimestampIndex] = -1L;
    }

    List<GPOMutable> keys = Lists.newArrayList(fullKey);

    GPOMutable value = new GPOMutable(agg.getAggregates());
    List<GPOMutable> values = Lists.newArrayList(value);

    metaData.getFieldsObject()[KEY_FD_INDEX] = fullKey.getFieldDescriptor();
    metaData.getFieldsObject()[AGGREGATE_FD_INDEX] = value.getFieldDescriptor();
    metaData.getFieldsObject()[KEYS_INDEX] = keys;
    metaData.getFieldsObject()[AGGREGATES_INDEX] = values;
    agg.setMetaData(metaData);

    return agg;
  }

  @Override
  public void aggregate(Aggregate dest, InputEvent src)
  {
    @SuppressWarnings("unchecked")
    List<GPOMutable> destKeys =
        (List<GPOMutable>)dest.getMetaData().getFieldsObject()[KEYS_INDEX];

    @SuppressWarnings("unchecked")
    List<GPOMutable> destAggregates =
        (List<GPOMutable>)dest.getMetaData().getFieldsObject()[AGGREGATES_INDEX];

    long timestamp = 0L;

    if (context.inputTimestampIndex >= 0) {
      timestamp = src.getKeys().getFieldsLong()[context.inputTimestampIndex];
      src.getKeys().getFieldsLong()[context.inputTimestampIndex] = -1L;
    }

    if (!contains(destKeys, src.getKeys())) {
      destKeys.add(new GPOMutable(src.getKeys()));

      GPOMutable aggregates = new GPOMutable(context.aggregateDescriptor);
      GPOUtils.indirectCopy(aggregates, src.getAggregates(), context.indexSubsetAggregates);

      destAggregates.add(aggregates);

      this.aggregateAggs(dest.getAggregates(), aggregates);
    }

    if (context.inputTimestampIndex >= 0) {
      src.getKeys().getFieldsLong()[context.inputTimestampIndex] = timestamp;
    }
  }

  @Override
  public void aggregate(Aggregate dest, Aggregate src)
  {
    dest.getMetaData().applyObjectPayloadFix();
    src.getMetaData().applyObjectPayloadFix();

    @SuppressWarnings("unchecked")
    List<GPOMutable> destKeys =
        (List<GPOMutable>)dest.getMetaData().getFieldsObject()[KEYS_INDEX];

    @SuppressWarnings("unchecked")
    List<GPOMutable> srcKeys =
        (List<GPOMutable>)src.getMetaData().getFieldsObject()[KEYS_INDEX];

    @SuppressWarnings("unchecked")
    List<GPOMutable> destAggregates =
        (List<GPOMutable>)dest.getMetaData().getFieldsObject()[AGGREGATES_INDEX];

    @SuppressWarnings("unchecked")
    List<GPOMutable> srcAggregates =
        (List<GPOMutable>)src.getMetaData().getFieldsObject()[AGGREGATES_INDEX];

    List<GPOMutable> newKeys = Lists.newArrayList();
    List<GPOMutable> newAggs = Lists.newArrayList();

    for (int index = 0;
        index < srcKeys.size();
        index++) {
      GPOMutable currentSrcKey = srcKeys.get(index);
      GPOMutable currentSrcAgg = srcAggregates.get(index);

      if (!contains(destKeys, currentSrcKey)) {
        newKeys.add(currentSrcKey);
        newAggs.add(currentSrcAgg);

        this.aggregateAggs(dest.getAggregates(), currentSrcAgg);
      }
    }

    destKeys.addAll(newKeys);
    destAggregates.addAll(newAggs);
  }

  private boolean contains(List<GPOMutable> mutables, GPOMutable mutable)
  {
    for (int index = 0;
        index < mutables.size();
        index++) {
      GPOMutable mutableFromList = mutables.get(index);

      if (GPOUtils.equals(mutableFromList, mutable)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public FieldsDescriptor getMetaDataDescriptor()
  {
    return META_DATA_FIELDS_DESCRIPTOR;
  }

  public static class PayloadFix implements SerdeObjectPayloadFix
  {
    @Override
    public void fix(Object[] objects)
    {
      FieldsDescriptor keyfd = (FieldsDescriptor)objects[KEY_FD_INDEX];
      FieldsDescriptor valuefd = (FieldsDescriptor)objects[AGGREGATE_FD_INDEX];

      @SuppressWarnings("unchecked")
      List<GPOMutable> keyMutables = (List<GPOMutable>)objects[KEYS_INDEX];
      @SuppressWarnings("unchecked")
      List<GPOMutable> aggregateMutables = (List<GPOMutable>)objects[AGGREGATES_INDEX];

      fix(keyfd, keyMutables);
      fix(valuefd, aggregateMutables);
    }

    private void fix(FieldsDescriptor fd, List<GPOMutable> mutables)
    {
      for (int index = 0;
          index < mutables.size();
          index++) {
        mutables.get(index).setFieldDescriptor(fd);
      }
    }
  }
}
