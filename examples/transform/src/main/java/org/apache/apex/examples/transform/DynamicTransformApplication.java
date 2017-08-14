/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.apex.examples.transform;

import java.util.Arrays;

import org.apache.apex.malhar.lib.partitioner.StatelessThroughputBasedPartitioner;
import org.apache.apex.malhar.lib.transform.TransformOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "DynamicTransformApp")
/**
 * @since 3.7.0
 */
public class DynamicTransformApplication extends SimpleTransformApplication
{
  private static String COOL_DOWN_MILLIS = "dt.cooldown";
  private static String MAX_THROUGHPUT = "dt.maxThroughput";
  private static String MIN_THROUGHPUT = "dt.minThroughput";

  @Override
  void setPartitioner(DAG dag,Configuration conf,TransformOperator transform)
  {
    StatelessThroughputBasedPartitioner<TransformOperator> partitioner = new StatelessThroughputBasedPartitioner<>();
    partitioner.setCooldownMillis(conf.getLong(COOL_DOWN_MILLIS, 10000));
    partitioner.setMaximumEvents(conf.getLong(MAX_THROUGHPUT, 30000));
    partitioner.setMinimumEvents(conf.getLong(MIN_THROUGHPUT, 10000));
    dag.setAttribute(transform, Context.OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{partitioner}));
    dag.setAttribute(transform, Context.OperatorContext.PARTITIONER, partitioner);
  }
}
