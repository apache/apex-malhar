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
import com.datatorrent.demos.dimensions.ads.AdInfo;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoSumAggregator;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdsDimensionsCombination;
import com.datatorrent.demos.dimensions.ads.generic.InputItemGenerator;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.DimensionsCombination;
import com.datatorrent.lib.dimensions.DimensionsComputationCustom;
import com.datatorrent.lib.dimensions.DimensionsComputationUnifierImpl;
import com.datatorrent.lib.dimensions.aggregator.Aggregator;
import com.datatorrent.lib.stream.DevNull;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;

import java.util.LinkedHashMap;
import java.util.List;

@ApplicationAnnotation(name="AdsDimensionsCustomBenchmark")
public class AdsDimensionsCustomBenchmark implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);
    DimensionsComputationCustom<AdInfo, AdInfo.AdInfoAggregateEvent> dimensions = dag.addOperator("DimensionsComputation", new DimensionsComputationCustom<AdInfo, AdInfo.AdInfoAggregateEvent>());
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 60);
    DevNull<Object> devNull = dag.addOperator("DevNull", new DevNull<Object>());

    input.setEventSchemaJSON(SchemaUtils.jarResourceFileToString("adsBenchmarkSchema.json"));

    String[] dimensionSpecs = new String[] {
      "time=" + "1m",
      "time=" + "1m" + ":location",
      "time=" + "1m" + ":advertiser",
      "time=" + "1m" + ":publisher",
      "time=" + "1m" + ":advertiser:location",
      "time=" + "1m" + ":publisher:location",
      "time=" + "1m" + ":publisher:advertiser",
      "time=" + "1m" + ":publisher:advertiser:location"
    };

    LinkedHashMap<String, DimensionsCombination<AdInfo, AdInfo.AdInfoAggregateEvent>> dimensionsCombinations =
    Maps.newLinkedHashMap();

    LinkedHashMap<String, List<Aggregator<AdInfo, AdInfo.AdInfoAggregateEvent>>> dimensionsAggregators =
    Maps.newLinkedHashMap();

    for(int index = 0;
        index < dimensionSpecs.length;
        index++) {
      String dimensionSpec = dimensionSpecs[index];
      AdsDimensionsCombination dimensionsCombination = new AdsDimensionsCombination();
      dimensionsCombination.init(dimensionSpec);
      dimensionsCombinations.put(dimensionSpec, dimensionsCombination);

      List<Aggregator<AdInfo, AdInfo.AdInfoAggregateEvent>> aggregators = Lists.newArrayList();
      AdInfoSumAggregator adInfoSumAggregator = new AdInfoSumAggregator();
      aggregators.add(adInfoSumAggregator);
      dimensionsAggregators.put(dimensionSpec, aggregators);
    }

    DimensionsComputationUnifierImpl<AdInfo, AdInfo.AdInfoAggregateEvent> unifier = new DimensionsComputationUnifierImpl<AdInfo, AdInfo.AdInfoAggregateEvent>();
    dimensions.setUnifier(unifier);
    dimensions.setDimensionsCombinations(dimensionsCombinations);
    dimensions.setAggregators(dimensionsAggregators);

    dag.addStream("InputStream", input.outputPort, dimensions.data).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, devNull.data);
  }
}
