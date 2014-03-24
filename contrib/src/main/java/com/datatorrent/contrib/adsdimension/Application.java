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
package com.datatorrent.contrib.adsdimension;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.contrib.adsdimension.AdInfo.AdInfoAggregator;
import com.datatorrent.lib.statistics.DimensionsComputation;

/**
 * <p>Application class.</p>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name="AdsDimension")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, "AdsDimension");

    InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);

    DimensionsComputation<AdInfo> dimensions = dag.addOperator("DimensionsComputation", new DimensionsComputation<AdInfo>());
    String[] dimensionSpecs = new String[] {
      "time=" + TimeUnit.MINUTES,
      "time=" + TimeUnit.MINUTES + ":adUnit",
      "time=" + TimeUnit.MINUTES + ":advertiserId",
      "time=" + TimeUnit.MINUTES + ":publisherId",
      "time=" + TimeUnit.MINUTES + ":advertiserId:adUnit",
      "time=" + TimeUnit.MINUTES + ":publisherId:adUnit",
      "time=" + TimeUnit.MINUTES + ":publisherId:advertiserId",
      "time=" + TimeUnit.MINUTES + ":publisherId:advertiserId:adUnit"
    };

    AdInfoAggregator[] aggregators = new AdInfoAggregator[dimensionSpecs.length];
    for (int i = dimensionSpecs.length; i-- > 0;) {
      AdInfoAggregator aggregator = new AdInfoAggregator();
      aggregator.init(dimensionSpecs[i]);
      aggregators[i] = aggregator;
    }

    dimensions.setAggregators(aggregators);

    RedisAggregateOutputOperator redis = dag.addOperator("Redis", new RedisAggregateOutputOperator());

    dag.addStream("InputStream", input.outputPort, dimensions.data).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, redis.input);
  }

}
