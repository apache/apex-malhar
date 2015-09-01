/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.ads.benchmark;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.demos.dimensions.ads.AdInfo;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregator;
import com.datatorrent.demos.dimensions.ads.InputItemGenerator;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;
import com.datatorrent.lib.stream.DevNull;

/**
 * @since 3.1.0
 */

@ApplicationAnnotation(name="AdsDimensionsStatsBenchmark")
public class AdsDimensionsStatsBenchmark implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration c)
  {
    InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);
    DimensionsComputation<AdInfo, AdInfo.AdInfoAggregateEvent> dimensions = dag.addOperator("DimensionsComputation", new DimensionsComputation<AdInfo, AdInfo.AdInfoAggregateEvent>());
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 10);
    DevNull<Object> devNull = dag.addOperator("DevNull", new DevNull<Object>());

    input.setEventSchemaJSON(SchemaUtils.jarResourceFileToString("adsBenchmarkSchema.json"));

    String[] dimensionSpecs = new String[] {
      "time=" + TimeUnit.MINUTES,
      "time=" + TimeUnit.MINUTES + ":location",
      "time=" + TimeUnit.MINUTES + ":advertiser",
      "time=" + TimeUnit.MINUTES + ":publisher",
      "time=" + TimeUnit.MINUTES + ":advertiser:location",
      "time=" + TimeUnit.MINUTES + ":publisher:location",
      "time=" + TimeUnit.MINUTES + ":publisher:advertiser",
      "time=" + TimeUnit.MINUTES + ":publisher:advertiser:location"
    };

    AdInfoAggregator[] aggregators = new AdInfoAggregator[dimensionSpecs.length];
    for(int index = 0; index < dimensionSpecs.length; index++) {
      AdInfoAggregator aggregator = new AdInfoAggregator();
      aggregator.init(dimensionSpecs[index], index);
      aggregators[index] = aggregator;
    }

    dimensions.setAggregators(aggregators);
    DimensionsComputationUnifierImpl<AdInfo, AdInfo.AdInfoAggregateEvent> unifier = new DimensionsComputationUnifierImpl<AdInfo, AdInfo.AdInfoAggregateEvent>();
    unifier.setAggregators(aggregators);
    dimensions.setUnifier(unifier);

    dag.addStream("InputStream", input.outputPort, dimensions.data).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, devNull.data);
  }
}
