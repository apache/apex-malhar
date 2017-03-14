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
package org.apache.apex.malhar.flume.integration;

import java.util.Collection;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.flume.discovery.Discovery;
import org.apache.apex.malhar.flume.operator.AbstractFlumeInputOperator;
import org.apache.apex.malhar.flume.storage.EventCodec;
import org.apache.flume.Event;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;

/**
 * baseDir needs to be created in HDFS
 * Local zookeeper service needs to be running on default 127.0.0.1:2181
 * Local flume service needs to be running using src/test/resources/flume/conf/flume_zkdiscovery.conf configuration
 */
@Ignore
public class ApplicationDiscoveryTest implements StreamingApplication
{
  static int globalCount;

  public static class FlumeInputOperator extends AbstractFlumeInputOperator<Event>
  {
    public ZKStatsListner zkListener = new AbstractFlumeInputOperator.ZKStatsListner();
    private boolean first = true;


    @Override
    public Event convert(Event event)
    {
      return event;
    }


    @Override
    public Collection<Partition<AbstractFlumeInputOperator<Event>>> definePartitions(Collection<Partition<AbstractFlumeInputOperator<Event>>> partitions, PartitioningContext context)
    {
      if (first) {
        first = false;
        zkListener.setup(null);
      }
      Collection<Discovery.Service<byte[]>> addresses;
      addresses = zkListener.discover();
      discoveredFlumeSinks.set(addresses);

      return super.definePartitions(partitions, context);
    }
  }

  public static class Counter implements Operator
  {
    private int count;
    private transient Event event;
    public final transient DefaultInputPort<Event> input = new DefaultInputPort<Event>()
    {
      @Override
      public void process(Event tuple)
      {
        count++;
        event = tuple;
      }

    };

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
      if (event != null) {
        logger.info("total count = {}, tuple = {}", count, new String(event.getBody()));
      } else {
        logger.info("total count = {}, tuple = {}", count, event);
      }
      globalCount = count;
    }

    @Override
    public void setup(OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }

    private static final Logger logger = LoggerFactory.getLogger(Counter.class);
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(com.datatorrent.api.Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS, 1000);
    FlumeInputOperator flume = dag.addOperator("FlumeOperator", new FlumeInputOperator());
    flume.setCodec(new EventCodec());
    flume.zkListener.setConnectionString("127.0.0.1:2181");
    flume.zkListener.setBasePath("/flume/basepath");
    Counter counter = dag.addOperator("Counter", new Counter());

    dag.addStream("Slices", flume.output, counter.input).setLocality(Locality.CONTAINER_LOCAL);
  }

  @Test
  public void test()
  {
    try {
      LocalMode.runApp(this, 10000);
    } catch (Exception ex) {
      logger.warn("The dag seems to be not testable yet, if it's - remove this exception handling", ex);
    }
    //flume source sequence generator is set to 10 in flume configuration going to two source -> 20
    Assert.assertEquals(20, globalCount);
  }

  private static final Logger logger = LoggerFactory.getLogger(ApplicationDiscoveryTest.class);
}
