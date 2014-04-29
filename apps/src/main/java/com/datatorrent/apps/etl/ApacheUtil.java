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

import java.util.concurrent.TimeUnit;

public class ApacheUtil
{
  public static MapAggregator[] getDefaultAggregators()
  {
    MapAggregator apacheDimension1 = new MapAggregator();
    apacheDimension1.init("geoip_country_name");

    MapAggregator apacheDimension2 = new MapAggregator();
    apacheDimension2.init("geoip_city_name");

    MapAggregator apacheDimension3 = new MapAggregator();
    apacheDimension3.init("geoip_region_name");

    MapAggregator apacheDimension4 = new MapAggregator();
    apacheDimension4.init("agentinfo_name"); // browser

    MapAggregator apacheDimension5 = new MapAggregator();
    apacheDimension5.init("agentinfo_os");

    MapAggregator apacheDimension6 = new MapAggregator();
    apacheDimension6.init("agentinfo_device");

    MapAggregator apacheDimension7 = new MapAggregator();
    apacheDimension7.init("url");

    MapAggregator apacheDimension8 = new MapAggregator();
    apacheDimension8.init("clientip");

    MapAggregator apacheDimension9 = new MapAggregator();
    apacheDimension9.init("geoip_country_name:request");

    MapAggregator apacheDimension10 = new MapAggregator();
    apacheDimension10.init("agentinfo_name:url");

    MapAggregator apacheDimension11 = new MapAggregator();
    apacheDimension11.init("agentinfo_device:url");

    MapAggregator apacheDimension12 = new MapAggregator();
    apacheDimension12.init("clientip:geoip_country_name");

    MapAggregator apacheDimension13 = new MapAggregator();
    apacheDimension13.init("timestamp=" + TimeUnit.DAYS);

    MapAggregator apacheDimension14 = new MapAggregator();
    apacheDimension14.init("timestamp=" + TimeUnit.DAYS + ":agentinfo_device");

    MapAggregator[] aggrs = new MapAggregator[]{apacheDimension1, apacheDimension2, apacheDimension3,
      apacheDimension4, apacheDimension5, apacheDimension6, apacheDimension7, apacheDimension8, apacheDimension9,
      apacheDimension10, apacheDimension11, apacheDimension13, apacheDimension14,
    };
    return aggrs;
  }
}
