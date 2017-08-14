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

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.kudu.test.KuduClusterAvailabilityTestRule;
import org.apache.apex.malhar.kudu.test.KuduClusterTestContext;
import org.apache.apex.malhar.lib.helper.TestPortContext;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;
import static org.junit.Assert.assertEquals;

public class KuduCreateUpdateDeleteOutputOperatorTest extends KuduClientTestCommons
{
  @Rule
  public KuduClusterAvailabilityTestRule kuduClusterAvailabilityTestRule = new KuduClusterAvailabilityTestRule();


  private static final transient Logger LOG = LoggerFactory.getLogger(KuduCreateUpdateDeleteOutputOperatorTest.class);

  private final String APP_ID = "TestKuduOutputOperator";

  private final int OPERATOR_ID_FOR_KUDU_CRUD = 0;

  private BaseKuduOutputOperator simpleKuduOutputOperator;

  private OperatorContext contextForKuduOutputOperator;

  private TestPortContext testPortContextForKuduOutput;

  @KuduClusterTestContext(kuduClusterBasedTest = true)
  @Before
  public void setUpKuduOutputOperatorContext() throws Exception
  {
    Attribute.AttributeMap.DefaultAttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    contextForKuduOutputOperator = mockOperatorContext(OPERATOR_ID_FOR_KUDU_CRUD, attributeMap);
    simpleKuduOutputOperator = new BaseKuduOutputOperator();
    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, UnitTestTablePojo.class);
    testPortContextForKuduOutput = new TestPortContext(portAttributes);
    simpleKuduOutputOperator.setup(contextForKuduOutputOperator);
    simpleKuduOutputOperator.activate(contextForKuduOutputOperator);
    simpleKuduOutputOperator.input.setup(testPortContextForKuduOutput);
  }


  @KuduClusterTestContext(kuduClusterBasedTest = true)
  @Test
  public void processForUpdate() throws Exception
  {
    KuduExecutionContext<UnitTestTablePojo> newInsertExecutionContext = new KuduExecutionContext<>();
    UnitTestTablePojo unitTestTablePojo = new UnitTestTablePojo();
    unitTestTablePojo.setIntrowkey(2);
    unitTestTablePojo.setStringrowkey("two" + System.currentTimeMillis());
    unitTestTablePojo.setTimestamprowkey(System.currentTimeMillis());
    unitTestTablePojo.setBooldata(true);
    unitTestTablePojo.setFloatdata(3.2f);
    unitTestTablePojo.setStringdata("" + System.currentTimeMillis());
    unitTestTablePojo.setLongdata(System.currentTimeMillis() + 1);
    unitTestTablePojo.setTimestampdata(System.currentTimeMillis() + 2);
    unitTestTablePojo.setBinarydata(ByteBuffer.wrap("stringdata".getBytes()));
    newInsertExecutionContext.setMutationType(KuduMutationType.INSERT);
    newInsertExecutionContext.setPayload(unitTestTablePojo);

    simpleKuduOutputOperator.beginWindow(1);
    simpleKuduOutputOperator.input.process(newInsertExecutionContext);
    KuduExecutionContext<UnitTestTablePojo> updateExecutionContext = new KuduExecutionContext<>();
    UnitTestTablePojo updatingRecord = new UnitTestTablePojo();
    updateExecutionContext.setMutationType(KuduMutationType.UPDATE);
    updatingRecord.setBooldata(false);
    updatingRecord.setIntrowkey(unitTestTablePojo.getIntrowkey());
    updatingRecord.setStringrowkey(unitTestTablePojo.getStringrowkey());
    updatingRecord.setTimestamprowkey(unitTestTablePojo.getTimestamprowkey());
    updateExecutionContext.setPayload(updatingRecord);
    simpleKuduOutputOperator.input.process(updateExecutionContext);
    simpleKuduOutputOperator.endWindow();

    UnitTestTablePojo unitTestTablePojoRead = new UnitTestTablePojo();
    unitTestTablePojoRead.setIntrowkey(unitTestTablePojo.getIntrowkey());
    unitTestTablePojoRead.setStringrowkey(unitTestTablePojo.getStringrowkey());
    unitTestTablePojoRead.setTimestamprowkey(unitTestTablePojo.getTimestamprowkey());
    lookUpAndPopulateRecord(unitTestTablePojoRead);
    assertEquals(unitTestTablePojoRead.isBooldata(), false);
  }

  @KuduClusterTestContext(kuduClusterBasedTest = true)
  @Test
  public void processForUpsert() throws Exception
  {
    KuduExecutionContext<UnitTestTablePojo> upsertExecutionContext = new KuduExecutionContext<>();
    UnitTestTablePojo unitTestTablePojo = new UnitTestTablePojo();
    unitTestTablePojo.setIntrowkey(3);
    unitTestTablePojo.setStringrowkey("three" + System.currentTimeMillis());
    unitTestTablePojo.setTimestamprowkey(System.currentTimeMillis());
    unitTestTablePojo.setBooldata(false);
    unitTestTablePojo.setFloatdata(3.2f);
    unitTestTablePojo.setStringdata("" + System.currentTimeMillis());
    unitTestTablePojo.setLongdata(System.currentTimeMillis() + 1);
    unitTestTablePojo.setTimestampdata(System.currentTimeMillis() + 2);
    unitTestTablePojo.setBinarydata(ByteBuffer.wrap("stringdata".getBytes()));
    upsertExecutionContext.setMutationType(KuduMutationType.UPSERT);
    upsertExecutionContext.setPayload(unitTestTablePojo);

    simpleKuduOutputOperator.beginWindow(2);
    simpleKuduOutputOperator.input.process(upsertExecutionContext);
    upsertExecutionContext.setMutationType(KuduMutationType.UPSERT);
    unitTestTablePojo.setBooldata(true);
    simpleKuduOutputOperator.input.process(upsertExecutionContext);
    simpleKuduOutputOperator.endWindow();

    UnitTestTablePojo unitTestTablePojoRead = new UnitTestTablePojo();
    unitTestTablePojoRead.setIntrowkey(unitTestTablePojo.getIntrowkey());
    unitTestTablePojoRead.setStringrowkey(unitTestTablePojo.getStringrowkey());
    unitTestTablePojoRead.setTimestamprowkey(unitTestTablePojo.getTimestamprowkey());
    lookUpAndPopulateRecord(unitTestTablePojoRead);
    assertEquals(unitTestTablePojoRead.isBooldata(), true);
  }

  @KuduClusterTestContext(kuduClusterBasedTest = true)
  @Test
  public void processForDelete() throws Exception
  {
    KuduExecutionContext<UnitTestTablePojo> insertExecutionContext = new KuduExecutionContext<>();
    UnitTestTablePojo unitTestTablePojo = new UnitTestTablePojo();
    unitTestTablePojo.setIntrowkey(4);
    unitTestTablePojo.setStringrowkey("four" + System.currentTimeMillis());
    unitTestTablePojo.setTimestamprowkey(System.currentTimeMillis());
    unitTestTablePojo.setBooldata(false);
    unitTestTablePojo.setFloatdata(3.2f);
    unitTestTablePojo.setStringdata("" + System.currentTimeMillis());
    unitTestTablePojo.setLongdata(System.currentTimeMillis() + 1);
    unitTestTablePojo.setTimestampdata(System.currentTimeMillis() + 2);
    unitTestTablePojo.setBinarydata(ByteBuffer.wrap("stringdata".getBytes()));
    insertExecutionContext.setMutationType(KuduMutationType.INSERT);
    insertExecutionContext.setPayload(unitTestTablePojo);

    simpleKuduOutputOperator.beginWindow(3);
    simpleKuduOutputOperator.input.process(insertExecutionContext);
    KuduExecutionContext<UnitTestTablePojo> deleteExecutionContext = new KuduExecutionContext<>();
    UnitTestTablePojo unitTestTablePojoDelete = new UnitTestTablePojo();
    unitTestTablePojoDelete.setIntrowkey(unitTestTablePojo.getIntrowkey());
    unitTestTablePojoDelete.setStringrowkey(unitTestTablePojo.getStringrowkey());
    unitTestTablePojoDelete.setTimestamprowkey(unitTestTablePojo.getTimestamprowkey());
    deleteExecutionContext.setMutationType(KuduMutationType.DELETE);
    deleteExecutionContext.setPayload(unitTestTablePojoDelete);
    simpleKuduOutputOperator.input.process(deleteExecutionContext);
    simpleKuduOutputOperator.endWindow();

    UnitTestTablePojo unitTestTablePojoRead = new UnitTestTablePojo();
    unitTestTablePojoRead.setIntrowkey(unitTestTablePojo.getIntrowkey());
    unitTestTablePojoRead.setStringrowkey(unitTestTablePojo.getStringrowkey());
    unitTestTablePojoRead.setTimestamprowkey(unitTestTablePojo.getTimestamprowkey());
    lookUpAndPopulateRecord(unitTestTablePojoRead);
    assertEquals(unitTestTablePojoRead.getBinarydata(), null);
  }

  @KuduClusterTestContext(kuduClusterBasedTest = true)
  @Test
  public void processForInsert() throws Exception
  {
    KuduExecutionContext<UnitTestTablePojo> insertExecutionContext = new KuduExecutionContext<>();
    UnitTestTablePojo unitTestTablePojo = new UnitTestTablePojo();
    unitTestTablePojo.setIntrowkey(1);
    unitTestTablePojo.setStringrowkey("one" + System.currentTimeMillis());
    unitTestTablePojo.setTimestamprowkey(System.currentTimeMillis());
    unitTestTablePojo.setBooldata(true);
    unitTestTablePojo.setFloatdata(3.2f);
    unitTestTablePojo.setStringdata("" + System.currentTimeMillis());
    unitTestTablePojo.setLongdata(System.currentTimeMillis() + 1);
    unitTestTablePojo.setTimestampdata(System.currentTimeMillis() + 2);
    unitTestTablePojo.setBinarydata(ByteBuffer.wrap("stringdata".getBytes()));
    insertExecutionContext.setMutationType(KuduMutationType.INSERT);
    insertExecutionContext.setPayload(unitTestTablePojo);

    simpleKuduOutputOperator.beginWindow(0);
    simpleKuduOutputOperator.input.process(insertExecutionContext);
    simpleKuduOutputOperator.endWindow();

    UnitTestTablePojo unitTestTablePojoRead = new UnitTestTablePojo();
    unitTestTablePojoRead.setIntrowkey(unitTestTablePojo.getIntrowkey());
    unitTestTablePojoRead.setStringrowkey(unitTestTablePojo.getStringrowkey());
    unitTestTablePojoRead.setTimestamprowkey(unitTestTablePojo.getTimestamprowkey());
    lookUpAndPopulateRecord(unitTestTablePojoRead);
    assertEquals("" + unitTestTablePojoRead.getFloatdata(),"" + unitTestTablePojo.getFloatdata());
  }

}
