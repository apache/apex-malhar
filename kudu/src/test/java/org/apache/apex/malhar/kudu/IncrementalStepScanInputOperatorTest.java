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
package org.apache.apex.malhar.kudu;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.kudu.partitioner.KuduPartitionScanStrategy;
import org.apache.apex.malhar.kudu.scanner.KuduScanOrderStrategy;
import org.apache.apex.malhar.kudu.test.KuduClusterAvailabilityTestRule;
import org.apache.apex.malhar.kudu.test.KuduClusterTestContext;
import org.apache.apex.malhar.lib.helper.TestPortContext;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class IncrementalStepScanInputOperatorTest extends KuduClientTestCommons
{
  @Rule
  public KuduClusterAvailabilityTestRule kuduClusterAvailabilityTestRule = new KuduClusterAvailabilityTestRule();

  private static final Logger LOG = LoggerFactory.getLogger(IncrementalStepScanInputOperatorTest.class);

  public static final String APP_ID = "TestIncrementalScanInputOperator";
  public static final int OPERATOR_ID_FOR_ONE_TO_ONE_PARTITIONER = 1;

  protected Context.OperatorContext operatorContext;
  protected TestPortContext testPortContext;
  protected IncrementalStepScanInputOperator<UnitTestTablePojo,InputOperatorControlTuple>
      incrementalStepScanInputOperator;
  protected Collection<Partitioner.Partition<AbstractKuduInputOperator>> partitions;

  protected KuduPartitionScanStrategy partitonScanStrategy = KuduPartitionScanStrategy.MANY_TABLETS_PER_OPERATOR;

  protected KuduScanOrderStrategy scanOrderStrategy = KuduScanOrderStrategy.RANDOM_ORDER_SCANNER;

  protected  int numberOfKuduInputOperatorPartitions = 5;

  protected Partitioner.PartitioningContext partitioningContext;

  @KuduClusterTestContext(kuduClusterBasedTest = true)
  @Test
  public void testInit() throws Exception
  {
    Attribute.AttributeMap.DefaultAttributeMap attributeMapForInputOperator =
        new Attribute.AttributeMap.DefaultAttributeMap();
    attributeMapForInputOperator.put(DAG.APPLICATION_ID, APP_ID);
    operatorContext = mockOperatorContext(OPERATOR_ID_FOR_ONE_TO_ONE_PARTITIONER,
      attributeMapForInputOperator);

    Attribute.AttributeMap.DefaultAttributeMap portAttributesForInputOperator =
        new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributesForInputOperator.put(Context.PortContext.TUPLE_CLASS, UnitTestTablePojo.class);
    testPortContext = new TestPortContext(portAttributesForInputOperator);

    incrementalStepScanInputOperator = new IncrementalStepScanInputOperator(UnitTestTablePojo.class,
        "kuduincrementalstepscaninputoperator.properties");
    incrementalStepScanInputOperator.setNumberOfPartitions(numberOfKuduInputOperatorPartitions);
    incrementalStepScanInputOperator.setPartitionScanStrategy(partitonScanStrategy);
    incrementalStepScanInputOperator.setScanOrderStrategy(scanOrderStrategy);
    partitioningContext = new Partitioner.PartitioningContext()
    {
      @Override
      public int getParallelPartitionCount()
      {
        return numberOfKuduInputOperatorPartitions;
      }

      @Override
      public List<Operator.InputPort<?>> getInputPorts()
      {
        return null;
      }
    };
    partitions = incrementalStepScanInputOperator.definePartitions(
      new ArrayList(), partitioningContext);
    Iterator<Partitioner.Partition<AbstractKuduInputOperator>> iteratorForMeta = partitions.iterator();
    IncrementalStepScanInputOperator actualOperator =
        (IncrementalStepScanInputOperator)iteratorForMeta.next().getPartitionedInstance();
    // Adjust the bindings as if apex has completed the partioning.The runtime of the framework does this in reality
    incrementalStepScanInputOperator = actualOperator;
    incrementalStepScanInputOperator.setup(operatorContext);
    incrementalStepScanInputOperator.activate(operatorContext);
    //rewire parent operator to enable proper unit testing method calls
    incrementalStepScanInputOperator.getPartitioner().setPrototypeKuduInputOperator(incrementalStepScanInputOperator);
    incrementalStepScanInputOperator.getScanner().setParentOperator(incrementalStepScanInputOperator);
  }

}
