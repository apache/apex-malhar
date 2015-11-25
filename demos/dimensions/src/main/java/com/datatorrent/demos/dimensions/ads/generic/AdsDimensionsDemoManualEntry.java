/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.ads.generic;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * @since 3.1.0
 */
@ApplicationAnnotation(name = AdsDimensionsDemoManualEntry.APP_NAME)
public class AdsDimensionsDemoManualEntry extends AdsDimensionsDemo
{
  public static final String APP_NAME = "AdsDimensionsDemoGenericManualEntry";
  public static final String EVENT_SCHEMA_LOCATION = "adsGenericEventSchemaNoEnums.json";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    this.appName = APP_NAME;
    this.eventSchemaLocation = EVENT_SCHEMA_LOCATION;
    this.advertisers = (List)Lists.newArrayList("starbucks","safeway","mcdonalds","macys","taco bell","walmart","khol's","san diego zoo","pandas","jack in the box","tomatina","ron swanson");
    super.populateDAG(dag, conf);
  }
}
