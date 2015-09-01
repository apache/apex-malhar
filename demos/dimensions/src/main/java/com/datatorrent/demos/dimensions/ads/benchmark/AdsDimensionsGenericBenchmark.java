/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.ads.benchmark;

import java.util.Map;

import com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;
import com.datatorrent.lib.stream.DevNull;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.demos.dimensions.ads.InputItemGenerator;

/**
 * @since 3.1.0
 */

@ApplicationAnnotation(name="AdsDimensionsGenericBenchmark")
public class AdsDimensionsGenericBenchmark implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);
    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = dag.addOperator("DimensionsComputation", DimensionsComputationFlexibleSingleSchemaPOJO.class);
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 10);
    DevNull<Object> devNull = dag.addOperator("DevNull", new DevNull<Object>());

    //Set input properties
    String eventSchema = SchemaUtils.jarResourceFileToString("adsBenchmarkSchema.json");
    input.setEventSchemaJSON(eventSchema);

    Map<String, String> keyToExpression = Maps.newHashMap();
    keyToExpression.put("publisher", "getPublisher()");
    keyToExpression.put("advertiser", "getAdvertiser()");
    keyToExpression.put("location", "getLocation()");
    keyToExpression.put("time", "getTime()");

    Map<String, String> aggregateToExpression = Maps.newHashMap();
    aggregateToExpression.put("cost", "getCost()");
    aggregateToExpression.put("revenue", "getRevenue()");
    aggregateToExpression.put("impressions", "getImpressions()");
    aggregateToExpression.put("clicks", "getClicks()");

    DimensionsComputationUnifierImpl<InputEvent, Aggregate> unifier = new DimensionsComputationUnifierImpl<InputEvent, Aggregate>();
    dimensions.setUnifier(unifier);
    dimensions.setKeyToExpression(keyToExpression);
    dimensions.setAggregateToExpression(aggregateToExpression);
    dimensions.setConfigurationSchemaJSON(eventSchema);

    dag.addStream("InputStream", input.outputPort, dimensions.input).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, devNull.data);
  }
}
