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
package com.datatorrent.lib.partitioner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.StringCodec.Object2String;
import com.datatorrent.common.util.BaseOperator;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;

public class ClonePartitionerTest
{
  private static Logger LOG = LoggerFactory.getLogger(ClonePartitionerTest.class);

  private static int numPartitions = 20;
  private static Integer countPerWindow = 100;
  private static boolean matchFailed = false;

  public static class FixedEmitter extends BaseOperator implements InputOperator
  {
    public final DefaultOutputPort<Integer> output = new DefaultOutputPort<>();

    private Integer emitted;

    @Override
    public void beginWindow(long windowId)
    {
      emitted = 0;
    }

    @Override
    public void emitTuples()
    {
      while (emitted < countPerWindow) {
        output.emit(emitted++);
      }
    }
  }

  public static class DummyOperator extends BaseOperator
  {
    private Integer value;

    private transient ArrayList<Integer> valuesReceived = new ArrayList<>();

    public DummyOperator()
    {
    }

    public DummyOperator(Integer value)
    {
      this.value = value;
    }

    public final transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
    {
      @Override
      public void process(Integer tuple)
      {
        valuesReceived.add(tuple);
      }
    };

    @Override
    public void beginWindow(long windowId)
    {
      valuesReceived.clear();
    }

    @Override
    public void endWindow()
    {
      if (valuesReceived.size() < countPerWindow) {
        matchFailed = true;
      } else {
        for (int i = 0; i < countPerWindow; i++) {
          if (valuesReceived.get(i) != i) {
            matchFailed = true;
          }
        }
      }
    }

    public void setValue(int value)
    {
      this.value = value;
    }

    public int getValue()
    {
      return value;
    }
  }

  @Test
  public void partition1Test()
  {
    DummyOperator dummyOperator = new DummyOperator(5);
    ClonePartitioner<DummyOperator> clonePartitioner = new ClonePartitioner<>();

    Collection<Partition<DummyOperator>> partitions = Lists.newArrayList();
    DefaultPartition<DummyOperator> defaultPartition = new DefaultPartition<>(dummyOperator);
    partitions.add(defaultPartition);

    Collection<Partition<DummyOperator>> newPartitions = clonePartitioner.definePartitions(partitions,
        new PartitioningContextImpl(null, 0));
    assertEquals("Incorrect number of partitions", 1, newPartitions.size());

    for (Partition<DummyOperator> partition : newPartitions) {
      assertEquals("Incorrect cloned value", 5, partition.getPartitionedInstance().getValue());
    }
  }

  @Test
  public void partition5Test()
  {
    DummyOperator dummyOperator = new DummyOperator(5);
    ClonePartitioner<DummyOperator> clonePartitioner = new ClonePartitioner<>(5);

    Collection<Partition<DummyOperator>> partitions = Lists.newArrayList();
    DefaultPartition<DummyOperator> defaultPartition = new DefaultPartition<>(dummyOperator);
    partitions.add(defaultPartition);

    Collection<Partition<DummyOperator>> newPartitions = clonePartitioner.definePartitions(partitions,
        new PartitioningContextImpl(null, 0));
    assertEquals("Incorrect number of partitions", 5, newPartitions.size());

    for (Partition<DummyOperator> partition : newPartitions) {
      assertEquals("Incorrect cloned value", 5, partition.getPartitionedInstance().getValue());
    }
  }

  @Test
  public void objectPropertyTest()
  {
    Object2String<ClonePartitioner<DummyOperator>> propertyReader = new Object2String<>();
    ClonePartitioner<DummyOperator> partitioner =
        propertyReader.fromString("com.datatorrent.lib.partitioner.ClonePartitioner:3");
    assertEquals(3, partitioner.getPartitionCount());
  }

  @Test
  public void parallelPartitionScaleUpTest()
  {
    DummyOperator dummyOperator = new DummyOperator(5);
    ClonePartitioner<DummyOperator> clonePartitioner = new ClonePartitioner<>();

    Collection<Partition<DummyOperator>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<>(dummyOperator));

    Collection<Partition<DummyOperator>> newPartitions = clonePartitioner.definePartitions(partitions,
        new PartitioningContextImpl(null, 5));
    assertEquals("after partition", 5, newPartitions.size());
  }

  @Test
  public void parallelPartitionScaleDownTest()
  {
    DummyOperator dummyOperator = new DummyOperator(5);
    ClonePartitioner<DummyOperator> clonePartitioner = new ClonePartitioner<>();

    Collection<Partition<DummyOperator>> partitions = Lists.newArrayList();

    for (int i = 5; i-- > 0; ) {
      partitions.add(new DefaultPartition<>(dummyOperator));
    }

    Collection<Partition<DummyOperator>> newPartitions = clonePartitioner.definePartitions(partitions,
        new PartitioningContextImpl(null, 1));
    assertEquals("after partition", 1, newPartitions.size());
  }

  static class Application implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      LOG.debug("Application - PopulateDAG");
      FixedEmitter emitter = new FixedEmitter();
      DummyOperator dummy = new DummyOperator(5);

      dag.addOperator("Emitter", emitter);
      dag.addOperator("Dummy", dummy);

      ClonePartitioner<DummyOperator> partitioner = new ClonePartitioner<>();
      partitioner.setPartitionCount(numPartitions);

      dag.setAttribute(dummy, Context.OperatorContext.PARTITIONER, partitioner);
      dag.addStream("Emitter to dummy", emitter.output, dummy.input);
    }
  }

  @Test
  public void testCloningAcrossPartitions() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();

      lc.run(20000); // runs for 10 seconds and quits
      assertFalse("Failed to match all values in all partitions.", matchFailed);
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  public static class PartitioningContextImpl implements Partitioner.PartitioningContext
  {
    final int parallelPartitionCount;
    final List<InputPort<?>> ports;

    public PartitioningContextImpl(List<InputPort<?>> ports, int parallelPartitionCount)
    {
      this.ports = ports;
      this.parallelPartitionCount = parallelPartitionCount;
    }

    @Override
    public int getParallelPartitionCount()
    {
      return parallelPartitionCount;
    }

    @Override
    public List<InputPort<?>> getInputPorts()
    {
      return ports;
    }
  }
}
