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

import java.util.Map;
import java.util.Set;

import org.apache.apex.malhar.lib.appdata.schemas.FieldsDescriptor;
import org.apache.apex.malhar.lib.appdata.schemas.Type;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.Aggregate;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.EventKey;

/**
 * @since 3.4.0
 */
public interface CompositeAggregator
{
  public int getSchemaID();

  public int getDimensionDescriptorID();

  public int getAggregatorID();

  public Set<Integer> getEmbedAggregatorDdIds();

  public Set<String> getFields();

  public FieldsDescriptor getAggregateDescriptor();

  public FieldsDescriptor getMetaDataDescriptor();

  /**
   * Returns the output type of the {@link CompositeAggregator}. <b>Note<b> that any combination of input types
   * will produce the same output type for {@link CompositeAggregator}s.
   * @return The output type of the {@link CompositeAggregator}.
   */
  public Type getOutputType();

  /**
   *
   * @param resultAggregate the aggregate to put the result
   * @param inputEventKeys The input(incremental) event keys, used to locate the input aggregates
   * @param inputAggregatesRepo: the map of the EventKey to Aggregate keep the super set of aggregate required
   */
  public void aggregate(Aggregate resultAggregate, Set<EventKey> inputEventKeys, Map<EventKey,
      Aggregate> inputAggregatesRepo);
}
