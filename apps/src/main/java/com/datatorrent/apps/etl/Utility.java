/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.apps.etl;

import java.util.List;

import com.google.common.collect.Lists;

import com.datatorrent.lib.datamodel.operation.CountOperation;
import com.datatorrent.lib.datamodel.operation.SumOperation;

public class Utility
{
  /**
   * For apache logs the default metrics for each combinations are:
   * <ul>
   *   <li>Sum of bytes.</li>
   *   <li>Count of hits.</li>
   *   <li>Average response time.</li>
   * </ul>
   * @return
   */
  static MapAggregator[] getDefaultApacheDimensionAggregators()
  {
    CountOperation countOperation = new CountOperation();
    SumOperation sumOperation = new SumOperation();

    Metric bytesSumMetric = new Metric("bytes", "bytesSum", sumOperation);
    Metric countHitsMetric = new Metric("url", "count", countOperation);
    Metric AverageResponseMetric = new Metric("responseTime", "avgResponse", )


    List<MapAggregator> specs = Lists.newArrayList();

    MapAggregator dim1 = new MapAggregator();
    dim1.init("geoip_country_name", Lists.newArrayList());
    specs.add();
    specs.add("geoip_city_name");
    specs.add("geoip_region_name");
    specs.add("agentinfo_name");

  }
}
