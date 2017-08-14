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
package org.apache.apex.malhar.contrib.aerospike;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.contrib.aerospike.AerospikeTestUtils.TestPOJO;

/**
 * Tests {@link AerospikePOJONonTransactionalPutOperator}
 */
public class AerospikePOJONonTransactionalPutOperatorTest
{

  private static final String APP_ID = "AerospikeNonTransactionalPutOperatorTest";

  @Test
  public void TestAerospikeNonTransactionalPutOperator()
  {
    AerospikeTestUtils.cleanTable();

    AerospikePOJONonTransactionalPutOperator outputOperator = new AerospikePOJONonTransactionalPutOperator();
    outputOperator.setStore(AerospikeTestUtils.getStore());
    outputOperator.setExpressions(AerospikeTestUtils.getExpressions());
    outputOperator.setup(AerospikeTestUtils.getOperatorContext(APP_ID));

    List<TestPOJO> events = AerospikeTestUtils.getEvents();

    outputOperator.beginWindow(0);
    for (TestPOJO event : events) {
      outputOperator.input.process(event);
    }
    outputOperator.endWindow();

    // check records
    Assert.assertTrue("key and value check", AerospikeTestUtils.checkEvents());
  }

}
