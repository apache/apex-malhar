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
package org.apache.apex.malhar.kudu.partitioner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.apex.malhar.kudu.AbstractKuduInputOperator;
import org.apache.apex.malhar.kudu.InputOperatorControlTuple;
import org.apache.apex.malhar.kudu.KuduClientTestCommons;
import org.apache.apex.malhar.kudu.KuduInputOperatorCommons;
import org.apache.apex.malhar.kudu.UnitTestTablePojo;
import org.apache.apex.malhar.kudu.scanner.KuduPartitionScanAssignmentMeta;
import org.apache.apex.malhar.kudu.test.KuduClusterAvailabilityTestRule;
import org.apache.apex.malhar.kudu.test.KuduClusterTestContext;
import org.apache.apex.malhar.lib.util.KryoCloneUtils;

import com.datatorrent.api.Partitioner;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({KryoCloneUtils.class})
public class KuduOneToManyPartitionerTest extends KuduInputOperatorCommons
{
  @Rule
  public KuduClusterAvailabilityTestRule kuduClusterAvailabilityTestRule = new KuduClusterAvailabilityTestRule();

  @KuduClusterTestContext(kuduClusterBasedTest = false)
  @Test
  public void testAssignPartitions() throws Exception
  {
    AbstractKuduInputOperator<UnitTestTablePojo,InputOperatorControlTuple> mockedInputOperator =
        PowerMockito.mock(AbstractKuduInputOperator.class);
    when(mockedInputOperator.getNumberOfPartitions()).thenReturn(5);
    PowerMockito.mockStatic(KryoCloneUtils.class);
    when(KryoCloneUtils.cloneObject(mockedInputOperator)).thenReturn(mockedInputOperator);
    KuduOneToManyPartitioner kuduOneToManyPartitioner = new KuduOneToManyPartitioner(mockedInputOperator);
    buildMockWiring(mockedInputOperator, KuduClientTestCommons.TOTAL_KUDU_TABLETS_FOR_UNITTEST_TABLE);
    kuduOneToManyPartitioner.setPrototypeKuduInputOperator(mockedInputOperator);
    Map<Integer,List<KuduPartitionScanAssignmentMeta>> assignedPartitions = kuduOneToManyPartitioner.assign(
        kuduOneToManyPartitioner.getListOfPartitionAssignments(
        new ArrayList<Partitioner.Partition<AbstractKuduInputOperator>>(),
        partitioningContext),partitioningContext);
    assertThat(assignedPartitions.size(), is(5));
    for (List<KuduPartitionScanAssignmentMeta> eachOperatorassignment: assignedPartitions.values()) {
      assertThat(eachOperatorassignment.size(), lessThanOrEqualTo(3));
    }
    for (List<KuduPartitionScanAssignmentMeta> eachOperatorassignment: assignedPartitions.values()) {
      assertThat(eachOperatorassignment.size(), greaterThanOrEqualTo(2));
    }
  }

}
