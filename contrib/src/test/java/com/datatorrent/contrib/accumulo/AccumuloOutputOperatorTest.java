/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.accumulo;


import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.common.util.DTThrowable;
import static com.datatorrent.contrib.accumulo.AccumuloTestHelper.con;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import java.util.ArrayList;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;

public class AccumuloOutputOperatorTest {
  private static String APP_ID = "AccumuloOutputOperatorTest";
  private static int OPERATOR_ID = 0;

  private static final Logger logger = LoggerFactory
      .getLogger(AccumuloOutputOperatorTest.class);

  @Test
  public void testPut() throws Exception {

    //MockInstance instance = new MockInstance();
    Connector con = null;
    Instance instance = new ZooKeeperInstance("accumulo", "node28.morado.com");
    try {
       logger.debug("connecting..");
       con=instance.getConnector("root","");
       con.tableOperations().create("tabs");

       logger.debug("connection done..");
    } catch (AccumuloException e) {
      logger.error("error in test helper");
      DTThrowable.rethrow(e);
    } catch (AccumuloSecurityException e) {
      logger.error("error in test helper");
      DTThrowable.rethrow(e);
    }
    //AccumuloTestHelper.clearTable();
    AccumuloOutputOperator atleastOper = new AccumuloOutputOperator();

    atleastOper.getStore().setTableName("tabs");
    atleastOper.getStore().setZookeeperHost("node28.morado.com");
    atleastOper.getStore().setInstanceName("accumulo");
    atleastOper.getStore().setUserName("root");
    atleastOper.getStore().setPassword("root");
    ArrayList<String> expressions = new ArrayList<String>();
    expressions.add("getRow()");
    expressions.add("getColumnFamily()");
    expressions.add("getColumnQualifier()");
    expressions.add("getColumnVisibility()");
    expressions.add("getColumnValue()");
    expressions.add("getTimestamp()");    
    atleastOper.setExpressions(expressions);
    ArrayList<String> dataTypes = new ArrayList<String>();
    dataTypes.add("string");
    dataTypes.add("string");
    dataTypes.add("string");
    dataTypes.add("string");
    dataTypes.add("string");
    dataTypes.add("long");

    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);
    atleastOper.setup(new OperatorContext() {

      @Override
      public <T> T getValue(Attribute<T> key) {
        return null;
      }

      @Override
      public AttributeMap getAttributes() {
        return null;
      }

      @Override
      public int getId() {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public void setCounters(Object counters) {
        // TODO Auto-generated method stub

      }
    });
    atleastOper.beginWindow(0);
    AccumuloTuple a=new AccumuloTuple();
    a.setRow("E001");
    a.setColumnFamily("name");
    a.setColumnQualifier("bob");
    a.setColumnValue("0");
    a.setColumnVisibility("public");
    a.setTimestamp(System.currentTimeMillis());
    atleastOper.input.process(a);
    atleastOper.endWindow();
    AccumuloTuple tuple;

    tuple = AccumuloTestHelper
        .getAccumuloTuple("E001", "name", "bob");

    Assert.assertNotNull("Tuple", tuple);
    Assert.assertEquals("Tuple row", "E001", tuple.getRow());
    Assert.assertEquals("Tuple column family","name", tuple.getColumnFamily());
    Assert.assertEquals("Tuple column qualifier","bob", tuple.getColumnQualifier());
    Assert.assertEquals("Tuple column value", "0", tuple.getColumnValue());

  }

}

