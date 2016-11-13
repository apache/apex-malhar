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

import com.google.common.collect.Maps;

/**
 * The DefaultCompositeAggregatorFactory find the specific factory according to the aggregator type
 * and delegate to the specific factory.
 *
 *
 * @since 3.4.0
 */
public class DefaultCompositeAggregatorFactory implements CompositeAggregatorFactory
{
  public static final DefaultCompositeAggregatorFactory defaultInst = new DefaultCompositeAggregatorFactory()
      .addFactory(AggregatorTopBottomType.TOPN.name(), TopBottomAggregatorFactory.defaultInstance)
      .addFactory(AggregatorTopBottomType.BOTTOMN.name(), TopBottomAggregatorFactory.defaultInstance);

  protected Map<String, CompositeAggregatorFactory> factoryRepository = Maps.newHashMap();

  @Override
  public String getCompositeAggregatorName(String aggregatorType, String embedAggregatorName,
      Map<String, Object> properties)
  {
    return findSpecificFactory(aggregatorType).getCompositeAggregatorName(aggregatorType,
        embedAggregatorName, properties);
  }

  @Override
  public <T> CompositeAggregator createCompositeAggregator(String aggregatorType, String embedAggregatorName,
      Map<String, Object> properties)
  {
    return findSpecificFactory(aggregatorType).createCompositeAggregator(aggregatorType,
        embedAggregatorName, properties);
  }

  protected CompositeAggregatorFactory findSpecificFactory(String aggregatorType)
  {
    return factoryRepository.get(aggregatorType);
  }

  public DefaultCompositeAggregatorFactory addFactory(String aggregatorType, CompositeAggregatorFactory factory)
  {
    factoryRepository.put(aggregatorType, factory);
    return this;
  }
}
