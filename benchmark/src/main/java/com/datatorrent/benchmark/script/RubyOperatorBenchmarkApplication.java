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
package com.datatorrent.benchmark.script;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.benchmark.RandomMapOutput;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.contrib.ruby.RubyOperator;
import com.datatorrent.lib.testbench.RandomEventGenerator;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * Application to benchmark the performance of ruby operator.
 * The operator was tested on the DT cluster and the
 * number of tuples processed by the operator per second were around 11,500
 *
 * @since 1.0.4
 */
// Dependent on libjar: jruby-core-1.7.12.jar
@ApplicationAnnotation(name = "RubyOperatorBenchmarkApplication")
public class RubyOperatorBenchmarkApplication implements StreamingApplication
{
  public static final int QUEUE_CAPACITY = 16 * 1024;
  private final Locality locality = null;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());
    rand.setMaxvalue(3000);
    rand.setTuplesBlast(120);

    RandomMapOutput randMap = dag.addOperator("randMap", new RandomMapOutput());
    randMap.setKey("val");

    RubyOperator ruby = dag.addOperator("ruby", new RubyOperator());
    String setupScript = "def square(val)\n";
    setupScript += "  return val*val\nend\n";
    ruby.addSetupScript(setupScript);
    ruby.setInvoke("square");
    ruby.setPassThru(true);

    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
    dag.getMeta(console).getMeta(console.input).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.getMeta(ruby).getMeta(ruby.result).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("rand_randMap", rand.integer_data, randMap.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("randMap_ruby", randMap.map_data, ruby.inBindings).setLocality(locality);
    dag.addStream("ruby_console", ruby.result, console.input).setLocality(locality);
  }

}
