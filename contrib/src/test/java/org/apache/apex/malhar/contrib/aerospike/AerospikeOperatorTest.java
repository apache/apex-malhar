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

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.Statement;

import com.datatorrent.api.Context.OperatorContext;

import static org.apache.apex.malhar.contrib.aerospike.AerospikeTestUtils.NAMESPACE;
import static org.apache.apex.malhar.contrib.aerospike.AerospikeTestUtils.NODE;
import static org.apache.apex.malhar.contrib.aerospike.AerospikeTestUtils.NUM_TUPLES;
import static org.apache.apex.malhar.contrib.aerospike.AerospikeTestUtils.PORT;
import static org.apache.apex.malhar.contrib.aerospike.AerospikeTestUtils.SET_NAME;
import static org.apache.apex.malhar.contrib.aerospike.AerospikeTestUtils.cleanMetaTable;
import static org.apache.apex.malhar.contrib.aerospike.AerospikeTestUtils.cleanTable;
import static org.apache.apex.malhar.contrib.aerospike.AerospikeTestUtils.getNumOfEventsInStore;
import static org.apache.apex.malhar.contrib.aerospike.AerospikeTestUtils.getOperatorContext;
import static org.apache.apex.malhar.contrib.aerospike.AerospikeTestUtils.getStore;
import static org.apache.apex.malhar.contrib.aerospike.AerospikeTestUtils.getTransactionalStore;

/**
 * Tests for {@link AbstractAerosipkeTransactionalPutOperator} and {@link AbstractAerospikeGetOperator}
 */
public class AerospikeOperatorTest
{

  private static String APP_ID = "AerospikeOperatorTest";

  private static class TestEvent
  {
    int id;

    TestEvent(int id)
    {
      this.id = id;
    }
  }

  private static class TestOutputOperator extends AbstractAerospikeTransactionalPutOperator<TestEvent>
  {

    TestOutputOperator()
    {
      cleanTable();
      cleanMetaTable();
    }

    @Override
    protected Key getUpdatedBins(TestEvent tuple, List<Bin> bins) throws AerospikeException
    {

      Key key = new Key(NAMESPACE,SET_NAME,String.valueOf(tuple.id));
      bins.add(new Bin("ID",tuple.id));
      return key;
    }
  }

  private static class TestInputOperator extends AbstractAerospikeGetOperator<TestEvent>
  {
    TestInputOperator()
    {
      cleanTable();
    }

    @Override
    public TestEvent getTuple(Record record)
    {

      return new TestEvent(record.getInt("ID"));
    }

    @Override
    public Statement queryToRetrieveData()
    {

      Statement stmnt = new Statement();
      stmnt.setNamespace(NAMESPACE);
      stmnt.setSetName(SET_NAME);

      return stmnt;
    }

    public void insertEventsInTable(int numEvents)
    {

      AerospikeClient client = null;
      try {
        client = new AerospikeClient(NODE, PORT);
        Key key;
        Bin bin;
        for (int i = 0; i < numEvents; i++) {
          key = new Key(NAMESPACE,SET_NAME,String.valueOf(i));
          bin = new Bin("ID",i);
          client.put(null, key, bin);
        }
      } catch (AerospikeException e) {
        throw e;
      } finally {
        if (null != client) {
          client.close();
        }
      }
    }

  }

  @Test
  public void TestAerospikeOutputOperator()
  {
    AerospikeTransactionalStore transactionalStore = getTransactionalStore();
    OperatorContext context = getOperatorContext(APP_ID);
    TestOutputOperator outputOperator = new TestOutputOperator();

    outputOperator.setStore(transactionalStore);
    outputOperator.setup(context);

    List<TestEvent> events = new ArrayList<TestEvent>();
    for (int i = 0; i < NUM_TUPLES; i++) {
      events.add(new TestEvent(i));
    }

    outputOperator.beginWindow(0);
    for (TestEvent event : events) {
      outputOperator.input.process(event);
    }
    outputOperator.endWindow();

    Assert.assertEquals("rows in db", NUM_TUPLES, getNumOfEventsInStore());
  }

  @Test
  public void TestAerospikeInputOperator()
  {
    AerospikeStore store = getStore();
    OperatorContext context = getOperatorContext(APP_ID);
    TestInputOperator inputOperator = new TestInputOperator();

    inputOperator.setStore(store);
    inputOperator.insertEventsInTable(NUM_TUPLES);

    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(context);
    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    Assert.assertEquals("rows from db", NUM_TUPLES, sink.collectedTuples.size());
  }

}
