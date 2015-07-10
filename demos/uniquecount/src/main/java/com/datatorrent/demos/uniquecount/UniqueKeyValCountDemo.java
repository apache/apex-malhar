/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.uniquecount;


import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.converter.MapToKeyHashValuePairConverter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.util.KeyValPair;

import com.datatorrent.common.partitioner.StatelessPartitioner;

/**
 * <p>UniqueKeyValCountDemo class.</p>
 *
 * @since 1.0.2
 */
@ApplicationAnnotation(name="UniqueKeyValueCountDemo")
public class UniqueKeyValCountDemo implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration entries)
  {
        /* Generate random key-value pairs */
    RandomDataGenerator randGen = dag.addOperator("randomgen", new RandomDataGenerator());

        /* Initialize with three partition to start with */
    UniqueCounter<KeyValPair<String, Object>> uniqCount =
        dag.addOperator("uniqevalue", new UniqueCounter<KeyValPair<String, Object>>());
    MapToKeyHashValuePairConverter<KeyValPair<String, Object>, Integer> converter = dag.addOperator("converter", new MapToKeyHashValuePairConverter());
    uniqCount.setCumulative(false);
    dag.setAttribute(randGen, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<UniqueCounter<KeyValPair<String, Object>>>(3));

    ConsoleOutputOperator output = dag.addOperator("output", new ConsoleOutputOperator());

    dag.addStream("datain", randGen.outPort, uniqCount.data);
    dag.addStream("convert", uniqCount.count, converter.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("consoutput", converter.output, output.input);
  }
}
