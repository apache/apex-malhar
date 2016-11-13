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
 * this factory is implemented for support TOPN and BOTTOMN right now.
 * we are not clear what other composite aggregator could be, provide interface here.
 * assume Composite only embed one aggregate and with some properties
 *
 * @since 3.4.0
 */
public interface CompositeAggregatorFactory
{
  /**
   * check if aggregatorName is a valid composite aggregator name or not.
   * @param aggregatorName
   * @return
   */
  //public boolean isValidCompositeAggregatorName(String aggregatorName);

  /**
   * get composite aggregator name based on composite aggregator information
   * @param aggregatorType
   * @param embedAggregatorName
   * @param properties
   * @return
   */
  public String getCompositeAggregatorName(String aggregatorType, String embedAggregatorName,
      Map<String, Object> properties);

  /**
   * create composite aggregator name based on composite aggregator information
   * @param aggregatorType
   * @param embedAggregatorName
   * @param properties
   * @return
   */
  public <T> CompositeAggregator createCompositeAggregator(String aggregatorType, String embedAggregatorName,
      Map<String, Object> properties);

}
