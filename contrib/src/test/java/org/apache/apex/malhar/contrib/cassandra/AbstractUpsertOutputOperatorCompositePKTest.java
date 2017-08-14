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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;
import static org.junit.Assert.assertEquals;

/**
 * A simple test class that checks functionality when composite primary keys are present as table definitions
 */
public class AbstractUpsertOutputOperatorCompositePKTest
{


  public static final String APP_ID = "TestCassandraUpsertOperator";
  public static final int OPERATOR_ID_FOR_COMPOSITE_PRIMARY_KEYS = 2;

  CompositePrimaryKeyUpdateOperator compositePrimaryKeysOperator = null;
  OperatorContext contextForCompositePrimaryKeysOperator;
  TestPortContext testPortContextForCompositePrimaryKeys;

  @Before
  public void setupApexContexts() throws Exception
  {
    Attribute.AttributeMap.DefaultAttributeMap attributeMapForCompositePrimaryKey =
        new Attribute.AttributeMap.DefaultAttributeMap();
    attributeMapForCompositePrimaryKey.put(DAG.APPLICATION_ID, APP_ID);
    contextForCompositePrimaryKeysOperator = mockOperatorContext(OPERATOR_ID_FOR_COMPOSITE_PRIMARY_KEYS,
                attributeMapForCompositePrimaryKey);

    Attribute.AttributeMap.DefaultAttributeMap portAttributesForCompositePrimaryKeys =
        new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributesForCompositePrimaryKeys.put(Context.PortContext.TUPLE_CLASS, CompositePrimaryKeyRow.class);
    testPortContextForCompositePrimaryKeys = new TestPortContext(portAttributesForCompositePrimaryKeys);

    compositePrimaryKeysOperator = new CompositePrimaryKeyUpdateOperator();
    compositePrimaryKeysOperator.setup(contextForCompositePrimaryKeysOperator);
    compositePrimaryKeysOperator.activate(contextForCompositePrimaryKeysOperator);
    compositePrimaryKeysOperator.input.setup(testPortContextForCompositePrimaryKeys);
  }

  @Test
  public void testForCompositeRowKeyBasedTable() throws Exception
  {
    CompositePrimaryKeyRow aCompositeRowKey = new CompositePrimaryKeyRow();
    String userId = new String("user1" + System.currentTimeMillis());
    String employeeId = new String(userId + System.currentTimeMillis());
    int day = 1;
    int month = 12;
    int year = 2017;
    aCompositeRowKey.setDay(day);
    aCompositeRowKey.setMonth(month);
    aCompositeRowKey.setYear(year);
    aCompositeRowKey.setCurrentstatus("status" + System.currentTimeMillis());
    aCompositeRowKey.setUserid(userId);
    aCompositeRowKey.setEmployeeid(employeeId);
    UpsertExecutionContext<CompositePrimaryKeyRow> anUpdate = new UpsertExecutionContext<>();
    anUpdate.setPayload(aCompositeRowKey);
    compositePrimaryKeysOperator.beginWindow(12);
    compositePrimaryKeysOperator.input.process(anUpdate);
    compositePrimaryKeysOperator.endWindow();

    ResultSet results = compositePrimaryKeysOperator.session.execute(
        "SELECT * FROM unittests.userstatus WHERE userid = '" + userId + "' and day=" + day + " and month=" +
        month + " and year=" + year + " and employeeid='" + employeeId + "'");
    List<Row> rows = results.all();
    assertEquals(rows.size(), 1);
  }


}
