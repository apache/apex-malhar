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

import com.google.common.collect.Sets;

/**
 * @since 3.4.0
 */
public class TopBottomAggregatorFactory extends AbstractCompositeAggregatorFactory
{
  public static final String PROPERTY_NAME_EMBEDED_AGGREGATOR = "embededAggregator";
  public static final String PROPERTY_NAME_COUNT = "count";
  public static final String PROPERTY_NAME_SUB_COMBINATIONS = "subCombinations";

  public static final TopBottomAggregatorFactory defaultInstance = new TopBottomAggregatorFactory();

  @Override
  public <T> AbstractTopBottomAggregator createCompositeAggregator(String aggregatorType, String embedAggregatorName,
      Map<String, Object> properties)
  {
    return createTopBottomAggregator(aggregatorType, embedAggregatorName, getCount(properties),
        getSubCombinations(properties));
  }

  public <T> AbstractTopBottomAggregator createTopBottomAggregator(String aggregatorType, String embedAggregatorName,
      int count, String[] subCombinations)
  {
    AbstractTopBottomAggregator aggregator = null;
    if (AggregatorTopBottomType.TOPN == AggregatorTopBottomType.valueOf(aggregatorType)) {
      aggregator = new AggregatorTop();
    }
    if (AggregatorTopBottomType.BOTTOMN == AggregatorTopBottomType.valueOf(aggregatorType)) {
      aggregator = new AggregatorBottom();
    }
    if (aggregator == null) {
      throw new IllegalArgumentException("Invalid composite type: " + aggregatorType);
    }
    aggregator.setEmbedAggregatorName(embedAggregatorName);
    aggregator.setCount(count);
    aggregator.setSubCombinations(subCombinations);

    return aggregator;
  }

  protected int getCount(Map<String, Object> properties)
  {
    return Integer.valueOf((String)properties.get(PROPERTY_NAME_COUNT));
  }

  protected String[] getSubCombinations(Map<String, Object> properties)
  {
    return (String[])properties.get(PROPERTY_NAME_SUB_COMBINATIONS);
  }

  /**
   * The properties of TOP or BOTTOM are count and subCombinations.
   * count only have one value and subCombinations is a set of string, we can order combinations to simplify the name
   */
  @Override
  protected String getNamePartialForProperties(Map<String, Object> properties)
  {
    StringBuilder sb = new StringBuilder();
    String count = (String)properties.get(PROPERTY_NAME_COUNT);
    sb.append(count).append(PROPERTY_SEPERATOR);

    String[] subCombinations =  (String[])properties.get(PROPERTY_NAME_SUB_COMBINATIONS);
    Set<String> sortedSubCombinations = Sets.newTreeSet();
    for (String subCombination : subCombinations) {
      sortedSubCombinations.add(subCombination);
    }

    for (String subCombination : sortedSubCombinations) {
      sb.append(subCombination).append(PROPERTY_SEPERATOR);
    }

    //delete the last one (PROPERTY_SEPERATOR)
    return sb.deleteCharAt(sb.length() - 1).toString();
  }
}
