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

package org.apache.apex.examples.throttle;

import java.util.Collection;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "ThrottleApplication")
/**
 * @since 3.7.0
 */
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Creating an example application with three operators
    // The last operator is slowing down the DAG
    // With the use of the stats listener the input operator is slowed when the window difference crosses a threshold

    RandomNumberGenerator randomGenerator = dag.addOperator("RandomGenerator", RandomNumberGenerator.class);
    PassThroughOperator<Double> passThrough = dag.addOperator("PassThrough", PassThroughOperator.class);
    SlowDevNullOperator<Double> devNull = dag.addOperator("SlowNull", SlowDevNullOperator.class);

    // Important to use the same stats listener object for all operators so that we can centrally collect stats and make
    // the decision
    StatsListener statsListener = new ThrottlingStatsListener();
    Collection<StatsListener> statsListeners = Lists.newArrayList(statsListener);
    dag.setAttribute(randomGenerator, Context.OperatorContext.STATS_LISTENERS, statsListeners);
    dag.setAttribute(passThrough, Context.OperatorContext.STATS_LISTENERS, statsListeners);
    dag.setAttribute(devNull, Context.OperatorContext.STATS_LISTENERS, statsListeners);

    // Increase timeout for the slow operator, this specifies the maximum timeout for an operator to process a window
    // It is specified in number of windows, since 1 window is 500ms, 30 minutes is 30 * 60 * 2 = 3600 windows
    dag.setAttribute(devNull, Context.OperatorContext.TIMEOUT_WINDOW_COUNT, 3600);

    // If there are unifiers that are slow then set timeout for them
    // dag.setUnifierAttribute(passThrough.output, Context.OperatorContext.TIMEOUT_WINDOW_COUNT, 3600);

    dag.addStream("randomData", randomGenerator.out, passThrough.input);
    dag.addStream("passData", passThrough.output, devNull.input);
  }
}
