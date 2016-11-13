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

/**
 * @since 3.4.0
 */
public abstract class AbstractCompositeAggregatorFactory implements CompositeAggregatorFactory
{
  protected static final String NAME_TEMPLATE = "%s-%s-%s";
  protected static final String PROPERTY_SEPERATOR = "_";
  protected static final String PROPERTY_VALUE_SEPERATOR = "|";

  @Override
  public String getCompositeAggregatorName(String aggregatorType, String embededAggregatorName,
      Map<String, Object> properties)
  {
    return String.format(NAME_TEMPLATE, aggregatorType, embededAggregatorName, getNamePartialForProperties(properties));
  }

  protected String getNamePartialForProperties(Map<String, Object> properties)
  {
    if (properties.size() == 1) {
      return properties.values().iterator().next().toString();
    }
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      sb.append(entry.getKey()).append(PROPERTY_VALUE_SEPERATOR).append(entry.getValue()).append(PROPERTY_SEPERATOR);
    }
    //delete the last one (PROPERTY_SEPERATOR)
    return sb.deleteCharAt(sb.length() - 1).toString();
  }
}

