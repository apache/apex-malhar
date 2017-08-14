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
package org.apache.apex.examples.mrmonitor;

import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.testbench.SeedEventGenerator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Application
 *
 * @since 2.0.0
 */
@ApplicationAnnotation(name = "MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build

    SeedEventGenerator seedGen = dag.addOperator("seedGen", SeedEventGenerator.class);
    seedGen.setSeedStart(1);
    seedGen.setSeedEnd(10);
    seedGen.addKeyData("x", 0, 10);
    seedGen.addKeyData("y", 0, 100);

    ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());
    cons.setStringFormat("hello: %s");

    dag.addStream("seeddata", seedGen.val_list, cons.input).setLocality(Locality.CONTAINER_LOCAL);
  }
}
