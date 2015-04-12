/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.ads.generic;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.stream.DevNull;
import org.apache.hadoop.conf.Configuration;

import static com.datatorrent.demos.dimensions.ads.generic.ApplicationWithHDHT.EVENT_SCHEMA;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@ApplicationAnnotation(name=AdsDimensionComputationBenchmark.APP_NAME)
public class AdsDimensionComputationBenchmark implements StreamingApplication
{
  public static final String APP_NAME = "GenericAdsDimensionComputationBenchmark";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);
    AdsDimensionComputation dimensions = dag.addOperator("DimensionsComputation", new AdsDimensionComputation());
    DevNull<AggregateEvent> devNull = dag.addOperator("Sink", new DevNull<AggregateEvent>());

    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);

    String eventSchema = SchemaUtils.jarResourceFileToString(EVENT_SCHEMA);
    dimensions.setEventSchemaJSON(eventSchema);

    dag.addStream("InputStream", input.outputPort, dimensions.inputEvent);
    dag.addStream("Dev Null", dimensions.aggregateOutput, devNull.data);
  }
}
