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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.contrib.aerospike.AerospikeTestUtils.*;

import static com.datatorrent.contrib.aerospike.AerospikeTestUtils.*;


/**
 * Tests {@link AerospikePOJONonTransactionalPutOperator}
 */
public class AerospikePOJONonTransactionalPutOperatorTest
{

  private static final String APP_ID = "AerospikeNonTransactionalPutOperatorTest";

  @Test
  public void TestAerospikeNonTransactionalPutOperator() {

    cleanTable();

    AerospikePOJONonTransactionalPutOperator outputOperator = new AerospikePOJONonTransactionalPutOperator();
    outputOperator.setStore(getStore());
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
