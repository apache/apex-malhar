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

package com.datatorrent.demos.dimensions.ads.benchmark;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.demos.dimensions.ads.InputItemGenerator;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.dimensions.DimensionsComputationUnifierImpl;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.stream.DevNull;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

@ApplicationAnnotation(name="AdsDimensionsFlexibleBenchmark")
public class AdsDimensionsFlexibleBenchmark implements StreamingApplication
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
    dimensions.setEventSchemaJSON(eventSchema);

    dag.addStream("InputStream", input.outputPort, dimensions.inputEvent).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, devNull.data);
  }
}
