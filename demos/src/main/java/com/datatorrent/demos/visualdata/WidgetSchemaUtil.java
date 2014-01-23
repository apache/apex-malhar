/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.demos.visualdata;

import java.util.HashMap;

import com.google.common.collect.Maps;

/**
 * A util class return the json notation of the schema of the Demo UI Widget
 */
public class WidgetSchemaUtil
{
  
  public static String getSimpleSchema(){
    return "{type:'simple'}";
  }
  
  public static String getTimeseriesSchema(int min, int max)
  {
    return "{type:'timeseries',minValue:" + min + ",maxValue:" + max + "}";
  }

  public static String getPercentageSchema()
  {
    return "{type:'percentage'}";
  }

  public static String getTopNSchema(int n)
  {
    return "{type:'topN',n:" + n + "}";
  }
  
  public static HashMap<String, Number> createTimeSeriesData(long timestamp, Number value){
    HashMap<String, Number> timeseriesMap = Maps.newHashMapWithExpectedSize(2);
    timeseriesMap.put("timestamp", timestamp);
    timeseriesMap.put("value", value);
    return timeseriesMap;
  }

}
