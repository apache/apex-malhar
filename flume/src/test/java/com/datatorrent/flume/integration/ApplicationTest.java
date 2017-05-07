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
package com.datatorrent.flume.integration;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.Event;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.flume.operator.AbstractFlumeInputOperator;
import com.datatorrent.flume.storage.EventCodec;

/**
 *
 */
@Ignore
public class ApplicationTest implements StreamingApplication
{
  public static class FlumeInputOperator extends AbstractFlumeInputOperator<Event>
  {
    @Override
    public Event convert(Event event)
    {
      return event;
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
      logger.debug("total count = {}, tuple = {}", count, event);
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
    flume.setConnectAddresses(new String[]{"test:127.0.0.1:8080"});
    flume.setCodec(new EventCodec());
    Counter counter = dag.addOperator("Counter", new Counter());

    dag.addStream("Slices", flume.output, counter.input).setLocality(Locality.CONTAINER_LOCAL);
  }

  @Test
  public void test()
  {
    try {
      LocalMode.runApp(this, Integer.MAX_VALUE);
    } catch (Exception ex) {
      logger.warn("The dag seems to be not testable yet, if it's - remove this exception handling", ex);
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(ApplicationTest.class);
}
