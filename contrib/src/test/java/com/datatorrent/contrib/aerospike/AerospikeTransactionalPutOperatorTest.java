/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.aerospike;

import org.junit.Assert;
import org.junit.Test;
import java.util.List;

import com.datatorrent.contrib.aerospike.AerospikeTestUtils.TestPOJO;

import static com.datatorrent.contrib.aerospike.AerospikeTestUtils.checkEvents;
import static com.datatorrent.contrib.aerospike.AerospikeTestUtils.cleanTable;
import static com.datatorrent.contrib.aerospike.AerospikeTestUtils.cleanMetaTable;
import static com.datatorrent.contrib.aerospike.AerospikeTestUtils.getEvents;
import static com.datatorrent.contrib.aerospike.AerospikeTestUtils.getExpressions;
import static com.datatorrent.contrib.aerospike.AerospikeTestUtils.getOperatorContext;
import static com.datatorrent.contrib.aerospike.AerospikeTestUtils.getTransactionalStore;

/**
 * Tests {@link AerospikeTransactionalPutOperator}
 */
public class AerospikeTransactionalPutOperatorTest {

  private static final String APP_ID = "AerospikeTransactionalPutOperatorTest";

  @Test
  public void TestAerospikeTransactionalPutOperator() {

    cleanTable(); cleanMetaTable();

    AerospikeTransactionalPutOperator outputOperator = new AerospikeTransactionalPutOperator();
    outputOperator.setStore(getTransactionalStore());
    outputOperator.setExpressions(getExpressions());
    outputOperator.setup(getOperatorContext(APP_ID));

    List<TestPOJO> events = getEvents();

    outputOperator.beginWindow(0);
    for (TestPOJO event : events) {
      outputOperator.input.process(event);
    }
    outputOperator.endWindow();

    // check records
    Assert.assertTrue("key and value check", checkEvents());
  }

}
