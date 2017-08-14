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
package org.apache.apex.malhar.contrib.hbase;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class HBaseCsvMappingPutOperatorTest
{
  private static final Logger logger = LoggerFactory
      .getLogger(HBaseCsvMappingPutOperatorTest.class);

  @Test
  public void testPut() throws Exception
  {
    try {
      HBaseTestHelper.startLocalCluster();
      HBaseTestHelper.clearHBase();
      HBaseCsvMappingPutOperator csvMappingPutOperator = new HBaseCsvMappingPutOperator();

      csvMappingPutOperator.getStore().setTableName("table1");
      csvMappingPutOperator.getStore().setZookeeperQuorum("127.0.0.1");
      csvMappingPutOperator.getStore().setZookeeperClientPort(2181);
      String s = "patrick,fremont,cali,milinda";
      String s1 = "gillett,santaclara,cali,milindas";
      String s2 = "aventferry,raleigh,nc,milind";
      csvMappingPutOperator.setMappingString("colfam0.street,colfam0.city,colfam0.state,row");
      csvMappingPutOperator.setup(mockOperatorContext(0));
      csvMappingPutOperator.beginWindow(0);
      csvMappingPutOperator.input.process(s);
      csvMappingPutOperator.input.process(s1);
      csvMappingPutOperator.input.process(s2);
      csvMappingPutOperator.endWindow();
      HBaseTuple tuple;

      tuple = HBaseTestHelper
          .getHBaseTuple("milind", "colfam0", "street");

      Assert.assertNotNull("Tuple", tuple);
      Assert.assertEquals("Tuple row", tuple.getRow(), "milind");
      Assert.assertEquals("Tuple column family", tuple.getColFamily(),
          "colfam0");
      Assert.assertEquals("Tuple column name", tuple.getColName(),
          "street");
      Assert.assertEquals("Tuple column value", tuple.getColValue(),
          "aventferry");
    } catch (IOException e) {

      logger.error(e.getMessage());
    }
  }

}
