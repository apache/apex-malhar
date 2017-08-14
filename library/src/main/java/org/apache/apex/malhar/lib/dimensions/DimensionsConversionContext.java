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
package org.apache.apex.malhar.lib.dimensions;

import java.io.Serializable;

import org.apache.apex.malhar.lib.appdata.gpo.GPOUtils.IndexSubset;
import org.apache.apex.malhar.lib.appdata.schemas.FieldsDescriptor;

/**
 * This is a context object used to convert {@link InputEvent}s into aggregates
 * in {@link IncrementalAggregator}s.
 *
 * @since 3.3.0
 */
public class DimensionsConversionContext implements Serializable
{
  private static final long serialVersionUID = 201506151157L;

  public CustomTimeBucketRegistry customTimeBucketRegistry;
  /**
   * The schema ID for {@link Aggregate}s emitted by the
   * {@link org.apache.apex.malhar.lib.dimensions.aggregator.IncrementalAggregator}
   * s holding this context.
   */
  public int schemaID;
  /**
   * The dimensionsDescriptor ID for {@link Aggregate}s emitted by the
   * {@link org.apache.apex.malhar.lib.dimensions.aggregator.IncrementalAggregator}
   * s holding this context.
   */
  public int dimensionsDescriptorID;
  /**
   * The aggregator ID for {@link Aggregate}s emitted by the
   * {@link org.apache.apex.malhar.lib.dimensions.aggregator.IncrementalAggregator}
   * s holding this context.
   */
  public int aggregatorID;
  /**
   * The {@link DimensionsDescriptor} corresponding to the given dimension
   * descriptor id.
   */
  public DimensionsDescriptor dd;
  /**
   * The {@link FieldsDescriptor} for the aggregate of the {@link Aggregate}s
   * emitted by the
   * {@link org.apache.apex.malhar.lib.dimensions.aggregator.IncrementalAggregator}
   * s holding this context object.
   */
  public FieldsDescriptor aggregateDescriptor;
  /**
   * The {@link FieldsDescriptor} for the key of the {@link Aggregate}s emitted
   * by the
   * {@link org.apache.apex.malhar.lib.dimensions.aggregator.IncrementalAggregator}
   * s holding this context object.
   */
  public FieldsDescriptor keyDescriptor;
  /**
   * The index of the timestamp field within the key of {@link InputEvent}s
   * received by the
   * {@link org.apache.apex.malhar.lib.dimensions.aggregator.IncrementalAggregator}
   * s holding this context object. This is -1 if the {@link InputEvent} key has
   * no timestamp.
   */
  public int inputTimestampIndex;
  /**
   * The index of the timestamp field within the key of {@link Aggregate}s
   * emitted by the
   * {@link org.apache.apex.malhar.lib.dimensions.aggregator.IncrementalAggregator}
   * s holding this context object. This is -1 if the {@link Aggregate}'s key
   * has no timestamp.
   */
  public int outputTimestampIndex;
  /**
   * The index of the time bucket field within the key of {@link Aggregate}s
   * emitted by the
   * {@link org.apache.apex.malhar.lib.dimensions.aggregator.IncrementalAggregator}
   * s holding this context object. This is -1 if the {@link Aggregate}'s key
   * has no timebucket.
   */
  public int outputTimebucketIndex;
  /**
   * The {@link IndexSubset} object that is used to extract key values from
   * {@link InputEvent}s received by this aggregator.
   */
  public IndexSubset indexSubsetKeys;
  /**
   * The {@link IndexSubset} object that is used to extract aggregate values
   * from {@link InputEvent}s received by this aggregator.
   */
  public IndexSubset indexSubsetAggregates;

  /**
   * Constructor for creating conversion context.
   */
  public DimensionsConversionContext()
  {
    //Do nothing.
  }
}
