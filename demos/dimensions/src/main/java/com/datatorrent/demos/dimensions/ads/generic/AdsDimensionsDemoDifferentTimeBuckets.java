/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.ads.generic;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = AdsDimensionsDemoDifferentTimeBuckets.APP_NAME)
public class AdsDimensionsDemoDifferentTimeBuckets extends AdsDimensionsDemo
{
  public static final String APP_NAME = "AdsDimensionsDemoDifferentTimeBuckets";
  public static final String EVENT_SCHEMA_LOCATION = "adsGenericEventSchemaTimeBuckets.json";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    this.appName = APP_NAME;
    this.eventSchemaLocation = EVENT_SCHEMA_LOCATION;
    super.populateDAG(dag, conf);
  }
}
