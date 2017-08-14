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
package org.apache.apex.malhar.contrib.cassandra;

import java.util.List;
import org.junit.Before;
import org.junit.Test;

import org.apache.apex.malhar.lib.helper.TestPortContext;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;
import static org.junit.Assert.assertEquals;

/**
 * A simple test class that  tests the functionality when a table with counters is the sink for POJOS
 */
public class AbstractUpsertOutputOperatorCountersTest
{

  public static final String APP_ID = "TestCassandraUpsertOperator";
  public static final int OPERATOR_ID_FOR_COUNTER_COLUMNS = 1;

  CounterColumnUpdatesOperator counterUpdatesOperator = null;
  OperatorContext contextForCountersOperator;
  TestPortContext testPortContextForCounters;

  @Before
  public void setupApexContexts() throws Exception
  {
    Attribute.AttributeMap.DefaultAttributeMap attributeMapForCounters =
        new Attribute.AttributeMap.DefaultAttributeMap();
    attributeMapForCounters.put(DAG.APPLICATION_ID, APP_ID);
    contextForCountersOperator = mockOperatorContext(OPERATOR_ID_FOR_COUNTER_COLUMNS, attributeMapForCounters);

    Attribute.AttributeMap.DefaultAttributeMap portAttributesForCounters =
        new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributesForCounters.put(Context.PortContext.TUPLE_CLASS, CounterColumnTableEntry.class);
    testPortContextForCounters = new TestPortContext(portAttributesForCounters);

    counterUpdatesOperator = new CounterColumnUpdatesOperator();
    counterUpdatesOperator.setup(contextForCountersOperator);
    counterUpdatesOperator.activate(contextForCountersOperator);
    counterUpdatesOperator.input.setup(testPortContextForCounters);
  }

  @Test
  public void testForSingleRowInsertForCounterTables() throws Exception
  {

    CounterColumnTableEntry aCounterEntry = new CounterColumnTableEntry();
    String userId = new String("user1" + System.currentTimeMillis());
    aCounterEntry.setUserId(userId);
    aCounterEntry.setUpdatecount(3);
    UpsertExecutionContext<CounterColumnTableEntry> anUpdate = new UpsertExecutionContext<>();
    anUpdate.setOverridingConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
    anUpdate.setPayload(aCounterEntry);
    counterUpdatesOperator.beginWindow(9);
    counterUpdatesOperator.input.process(anUpdate);
    counterUpdatesOperator.endWindow();

    ResultSet results = counterUpdatesOperator.session.execute(
        "SELECT * FROM unittests.userupdates WHERE userid = '" + userId + "'");
    List<Row> rows = results.all();
    assertEquals(rows.size(), 1);
    assertEquals(3, rows.get(0).getLong("updatecount"));

    aCounterEntry = new CounterColumnTableEntry();
    aCounterEntry.setUserId(userId);
    aCounterEntry.setUpdatecount(2);
    anUpdate = new UpsertExecutionContext<>();
    anUpdate.setOverridingConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
    anUpdate.setPayload(aCounterEntry);
    counterUpdatesOperator.beginWindow(10);
    counterUpdatesOperator.input.process(anUpdate);
    counterUpdatesOperator.endWindow();
    results = counterUpdatesOperator.session.execute(
                "SELECT * FROM unittests.userupdates WHERE userid = '" + userId + "'");
    rows = results.all();
    assertEquals(5, rows.get(0).getLong("updatecount"));
    aCounterEntry = new CounterColumnTableEntry();
    aCounterEntry.setUserId(userId);
    aCounterEntry.setUpdatecount(-1);
    anUpdate = new UpsertExecutionContext<>();
    anUpdate.setOverridingConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
    anUpdate.setPayload(aCounterEntry);

    counterUpdatesOperator.beginWindow(11);
    counterUpdatesOperator.input.process(anUpdate);
    counterUpdatesOperator.endWindow();

    results = counterUpdatesOperator.session.execute(
                "SELECT * FROM unittests.userupdates WHERE userid = '" + userId + "'");
    rows = results.all();
    assertEquals(4, rows.get(0).getLong("updatecount"));

  }

}
