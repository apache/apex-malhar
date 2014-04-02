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
package com.datatorrent.contrib.kafka.benchmark;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * An stream app to produce msg to cluster
 *
 * @since 0.9.3
 */
@ApplicationAnnotation(name="KafkaOutputBenchmark")
public class KafkaOutputBenchmark implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, "KafkaProducerBenchmark");
    BenchmarkPartitionableKafkaOutputOperator bpkoo = dag.addOperator("KafkaBenchmarkProducer", BenchmarkPartitionableKafkaOutputOperator.class);
    bpkoo.setBrokerList(conf.get("kafka.brokerlist"));
    dag.setAttribute(bpkoo, OperatorContext.INITIAL_PARTITION_COUNT, 2);
  }

}
