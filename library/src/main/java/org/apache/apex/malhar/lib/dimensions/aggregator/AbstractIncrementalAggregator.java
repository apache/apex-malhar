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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.appdata.gpo.GPOMutable;
import org.apache.apex.malhar.lib.appdata.gpo.GPOUtils;
import org.apache.apex.malhar.lib.appdata.schemas.CustomTimeBucket;
import org.apache.apex.malhar.lib.dimensions.DimensionsConversionContext;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.Aggregate;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.EventKey;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.InputEvent;

import com.google.common.base.Preconditions;

/**
 * * <p>
 * {@link IncrementalAggregator}s perform aggregations in place, on a field by field basis. For example if we have a
 * field cost, an incremental aggregator would take a new value of cost and aggregate it to an aggregate value for
 * cost. No fields except the cost field are used in the computation of the cost aggregation in the case of an
 * {@link IncrementalAggregator}.
 * </p>
 * <p>
 * {@link IncrementalAggregator}s are intended to be used with subclasses of
 * {@link org.apache.apex.malhar.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema}. The way in which
 * {@link IncrementalAggregator}s are used in this context is that a batch of fields to be aggregated by the aggregator
 * are provided in the form of an {@link InputEvent}. For example, if there are two fields (cost and revenue), which
 * will be aggregated by a sum aggregator, both of those fields will be included in the {@link InputEvent} passed to
 * the sum aggregator. And the {DimensionsEventregate} event produced by the sum aggregator will contain two fields,
 * one for cost and one for revenue.
 * </p>
 *
 *
 * @since 3.4.0
 */
public abstract class AbstractIncrementalAggregator implements IncrementalAggregator
{
  private static final long serialVersionUID = 201506211153L;

  /**
   * The conversion context for this aggregator.
   */
  protected DimensionsConversionContext context;

  public AbstractIncrementalAggregator()
  {
  }

  @Override
  public void setDimensionsConversionContext(DimensionsConversionContext context)
  {
    this.context = Preconditions.checkNotNull(context);
  }

  @Override
  public Aggregate getGroup(InputEvent src, int aggregatorIndex)
  {
    src.used = true;
    Aggregate aggregate = createAggregate(src,
        context,
        aggregatorIndex);
    return aggregate;
  }

  @Override
  public int hashCode(InputEvent inputEvent)
  {
    long timestamp = -1L;
    boolean hasTime = this.context.inputTimestampIndex != -1
        && this.context.outputTimebucketIndex != -1;

    if (hasTime) {
      timestamp = inputEvent.getKeys().getFieldsLong()[this.context.inputTimestampIndex];
      inputEvent.getKeys().getFieldsLong()[this.context.inputTimestampIndex]
          = this.context.dd.getCustomTimeBucket().roundDown(timestamp);
    }

    int hashCode = GPOUtils.indirectHashcode(inputEvent.getKeys(), context.indexSubsetKeys);

    if (hasTime) {
      inputEvent.getKeys().getFieldsLong()[this.context.inputTimestampIndex] = timestamp;
    }

    return hashCode;
  }

  @Override
  public boolean equals(InputEvent inputEvent1, InputEvent inputEvent2)
  {
    long timestamp1 = 0;
    long timestamp2 = 0;

    if (context.inputTimestampIndex != -1) {
      timestamp1 = inputEvent1.getKeys().getFieldsLong()[context.inputTimestampIndex];
      inputEvent1.getKeys().getFieldsLong()[context.inputTimestampIndex] =
          context.dd.getCustomTimeBucket().roundDown(timestamp1);

      timestamp2 = inputEvent2.getKeys().getFieldsLong()[context.inputTimestampIndex];
      inputEvent2.getKeys().getFieldsLong()[context.inputTimestampIndex] =
          context.dd.getCustomTimeBucket().roundDown(timestamp2);
    }

    boolean equals = GPOUtils.subsetEquals(inputEvent2.getKeys(),
        inputEvent1.getKeys(),
        context.indexSubsetKeys);

    if (context.inputTimestampIndex != -1) {
      inputEvent1.getKeys().getFieldsLong()[context.inputTimestampIndex] = timestamp1;
      inputEvent2.getKeys().getFieldsLong()[context.inputTimestampIndex] = timestamp2;
    }

    return equals;
  }

  /**
   * Creates an {@link Aggregate} from the given {@link InputEvent}.
   *
   * @param inputEvent      The {@link InputEvent} to unpack into an {@link Aggregate}.
   * @param context         The conversion context required to transform the {@link InputEvent} into
   *                        the correct {@link Aggregate}.
   * @param aggregatorIndex The aggregatorIndex assigned to this {@link Aggregate}.
   * @return The converted {@link Aggregate}.
   */
  public static Aggregate createAggregate(InputEvent inputEvent,
      DimensionsConversionContext context,
      int aggregatorIndex)
  {
    GPOMutable aggregates = new GPOMutable(context.aggregateDescriptor);
    EventKey eventKey = createEventKey(inputEvent,
        context,
        aggregatorIndex);

    Aggregate aggregate = new Aggregate(eventKey,
        aggregates);
    aggregate.setAggregatorIndex(aggregatorIndex);

    return aggregate;
  }

  /**
   * Creates an {@link EventKey} from the given {@link InputEvent}.
   *
   * @param inputEvent      The {@link InputEvent} to extract an {@link EventKey} from.
   * @param context         The conversion context required to extract the {@link EventKey} from
   *                        the given {@link InputEvent}.
   * @param aggregatorIndex The aggregatorIndex to assign to this {@link InputEvent}.
   * @return The {@link EventKey} extracted from the given {@link InputEvent}.
   */
  public static EventKey createEventKey(InputEvent inputEvent,
      DimensionsConversionContext context,
      int aggregatorIndex)
  {
    GPOMutable keys = new GPOMutable(context.keyDescriptor);
    GPOUtils.indirectCopy(keys, inputEvent.getKeys(), context.indexSubsetKeys);

    if (context.outputTimebucketIndex >= 0) {
      CustomTimeBucket timeBucket = context.dd.getCustomTimeBucket();

      keys.getFieldsInteger()[context.outputTimebucketIndex] = context.customTimeBucketRegistry.getTimeBucketId(
          timeBucket);
      keys.getFieldsLong()[context.outputTimestampIndex] =
          timeBucket.roundDown(inputEvent.getKeys().getFieldsLong()[context.inputTimestampIndex]);
    }

    EventKey eventKey = new EventKey(context.schemaID,
        context.dimensionsDescriptorID,
        context.aggregatorID,
        keys);

    return eventKey;
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractIncrementalAggregator.class);
}
