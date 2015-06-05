/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.demos.dimensions.ads.generic;

import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

@ApplicationAnnotation(name=AdsDimensionsDemoManualEntry.APP_NAME)
public class AdsDimensionsDemoManualEntry extends AdsDimensionsDemo
{
  public static final String APP_NAME = "AdsDimensionsDemoGenericManualEntry-Customer";
  public static final String EVENT_SCHEMA_LOCATION = "adsGenericEventSchemaNoEnums.json";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    this.appName = APP_NAME;
    this.eventSchemaLocation = EVENT_SCHEMA_LOCATION;
    this.advertisers = (List) Lists.newArrayList("starbucks","safeway","mcdonalds","macys","taco bell","walmart","khol's","san diego zoo","pandas","jack in the box","tomatina","ron swanson");
    super.populateDAG(dag, conf);
  }
}
